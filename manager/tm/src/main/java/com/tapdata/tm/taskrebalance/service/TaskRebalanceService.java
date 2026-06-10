package com.tapdata.tm.taskrebalance.service;

import com.tapdata.manager.common.utils.DateUtil;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.taskrebalance.constant.TaskRebalanceJobStatus;
import com.tapdata.tm.taskrebalance.constant.TaskRebalanceStatus;
import com.tapdata.tm.taskrebalance.dto.TaskRebalanceDto;
import com.tapdata.tm.taskrebalance.dto.TaskRebalanceJobDto;
import com.tapdata.tm.taskrebalance.entity.TaskRebalanceEntity;
import com.tapdata.tm.taskrebalance.repository.TaskRebalanceRepository;
import com.tapdata.tm.taskrebalance.rule.TaskRebalanceRuleService;
import com.tapdata.tm.taskrebalance.vo.TaskRebalanceDetailVo;
import com.tapdata.tm.taskrebalance.vo.TaskRebalanceJobVo;
import com.tapdata.tm.taskrebalance.vo.TaskRebalancePreviewVo;
import com.tapdata.tm.taskrebalance.vo.TaskRebalanceVo;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import jakarta.annotation.PostConstruct;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TaskRebalanceService extends BaseService<TaskRebalanceDto, TaskRebalanceEntity, ObjectId, TaskRebalanceRepository> {
    private static final long DEFAULT_TASK_STATUS_TIMEOUT_MS = 5 * 60 * 1000L;
    private static final long POLL_INTERVAL_MS = 2000L;
    private static final int MAX_TARGET_AGENT_CONCURRENCY = 8;
    private static final long EXECUTION_WAIT_INTERVAL_MS = 1000L;

    private final TaskRebalanceJobService jobService;
    private final TaskService taskService;
    private final WorkerService workerService;
    private final UserService userService;
    private final SettingsService settingsService;
    private final TaskRebalanceRuleService ruleService;

    public TaskRebalanceService(@NonNull TaskRebalanceRepository repository,
                                TaskRebalanceJobService jobService,
                                TaskService taskService,
                                WorkerService workerService,
                                UserService userService,
                                SettingsService settingsService,
                                TaskRebalanceRuleService ruleService) {
        super(repository, TaskRebalanceDto.class, TaskRebalanceEntity.class);
        this.jobService = jobService;
        this.taskService = taskService;
        this.workerService = workerService;
        this.userService = userService;
        this.settingsService = settingsService;
        this.ruleService = ruleService;
    }

    @Override
    protected void beforeSave(TaskRebalanceDto dto, UserDetail userDetail) {
    }

    @PostConstruct
    public void logUnfinishedRebalancesOnStartup() {
        if (settingsService.isCloud()) {
            return;
        }
        Query query = Query.query(Criteria.where(TaskRebalanceDto.FIELD_STATUS).is(TaskRebalanceStatus.RUNNING));
        List<TaskRebalanceDto> rebalances = findAll(query);
        if (CollectionUtils.isEmpty(rebalances)) {
            return;
        }
        log.info("TaskRebalance found unfinished rebalances, count={}", rebalances.size());
    }

    public TaskRebalancePreviewVo preview(UserDetail userDetail) {
        assertRebalanceEnabled();
        assertRebalancePermission(userDetail);
        List<Worker> onlineWorkers = findAvailableAgents();
        if (CollectionUtils.isEmpty(onlineWorkers) || onlineWorkers.size() < 2) {
            TaskRebalancePreviewVo preview = new TaskRebalancePreviewVo();
            preview.setReason("task.rebalance.onlyOneAgent");
            return preview;
        }

        Map<String, Worker> workerMap = onlineWorkers.stream()
                .filter(worker -> StringUtils.isNotBlank(worker.getProcessId()))
                .collect(Collectors.toMap(Worker::getProcessId, worker -> worker, (a, b) -> a, LinkedHashMap::new));
        List<TaskDto> tasks = findStatTasks();
        TaskRebalancePreviewVo preview = buildPreview(tasks, workerMap);
        return preview;
    }

    /**
     * Builds a rebalance record from the preview submitted by the frontend, persists only
     * changed movable tasks, and deduplicates by task id before creating child jobs.
     */
    public TaskRebalanceVo createAndExecute(TaskRebalancePreviewVo submittedPreview, UserDetail userDetail) {
        assertRebalanceEnabled();
        assertRebalancePermission(userDetail);
        if (hasActive(userDetail)) {
            throw new BizException("task.rebalance.alreadyRunning");
        }
        if (submittedPreview == null || StringUtils.isNotBlank(submittedPreview.getReason())) {
            throw new BizException(submittedPreview == null ? "task.rebalance.noTask" : submittedPreview.getReason());
        }
        List<TaskRebalancePreviewVo.TaskPreview> changedTasks = collectSubmittedChangedTasks(submittedPreview);
        if (changedTasks.isEmpty()) {
            throw new BizException("task.rebalance.noTask");
        }
        validateSubmittedTasks(changedTasks);
        if (jobService.hasAnyActiveJob(changedTasks.stream().map(TaskRebalancePreviewVo.TaskPreview::getTaskId).collect(Collectors.toList()), userDetail)) {
            throw new BizException("task.rebalance.taskRebalancing");
        }

        TaskRebalanceDto rebalance = new TaskRebalanceDto();
        rebalance.setName(MessageFormat.format("Task rebalance {0}", DateUtil.now()));
        rebalance.setStatus(TaskRebalanceStatus.RUNNING);
        rebalance.setTotalCount(changedTasks.size());
        rebalance.setPendingCount(changedTasks.size());
        rebalance.setStoppingCount(0);
        rebalance.setStartingCount(0);
        rebalance.setOkCount(0);
        rebalance.setFailedCount(0);
        rebalance.setCancelledCount(0);
        rebalance = save(rebalance, userDetail);

        List<TaskRebalanceJobDto> jobs = new ArrayList<>();
        for (TaskRebalancePreviewVo.TaskPreview task : changedTasks) {
            TaskRebalanceJobDto job = new TaskRebalanceJobDto();
            job.setRebalanceId(rebalance.getId().toHexString());
            job.setTaskId(task.getTaskId());
            job.setTaskName(task.getTaskName());
            job.setStatus(TaskRebalanceJobStatus.PENDING);
            job.setSourceAgentId(task.getSourceAgentId());
            job.setTargetAgentId(task.getTargetAgentId());
            jobs.add(job);
        }
        jobService.save(jobs, userDetail);
        return toVo(rebalance);
    }

    private List<TaskRebalancePreviewVo.TaskPreview> collectSubmittedChangedTasks(TaskRebalancePreviewVo preview) {
        if (CollectionUtils.isEmpty(preview.getTasks())) {
            return new ArrayList<>();
        }
        return preview.getTasks().stream()
                .filter(task -> Boolean.TRUE.equals(task.getMovable()))
                .filter(task -> Boolean.TRUE.equals(task.getChanged()))
                .filter(task -> StringUtils.isNotBlank(task.getTaskId()))
                .collect(Collectors.collectingAndThen(
                        Collectors.toMap(TaskRebalancePreviewVo.TaskPreview::getTaskId, task -> task, (left, right) -> left, LinkedHashMap::new),
                        taskMap -> new ArrayList<>(taskMap.values())
                ));
    }

    private void validateSubmittedTasks(List<TaskRebalancePreviewVo.TaskPreview> submittedTasks) {
        List<Worker> onlineWorkers = findAvailableAgents();
        Set<String> onlineAgentIds = onlineWorkers.stream()
                .map(Worker::getProcessId)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet());
        if (onlineAgentIds.size() < 2) {
            throw new BizException("task.rebalance.onlyOneAgent");
        }
        for (TaskRebalancePreviewVo.TaskPreview submittedTask : submittedTasks) {
            validateSubmittedTask(submittedTask, onlineAgentIds);
        }
    }

    private void validateSubmittedTask(TaskRebalancePreviewVo.TaskPreview submittedTask, Set<String> onlineAgentIds) {
        if (StringUtils.isBlank(submittedTask.getSourceAgentId())
                || StringUtils.isBlank(submittedTask.getTargetAgentId())
                || Objects.equals(submittedTask.getSourceAgentId(), submittedTask.getTargetAgentId())
                || !onlineAgentIds.contains(submittedTask.getTargetAgentId())) {
            throw new BizException("task.rebalance.invalidPreview");
        }
        if (!ObjectId.isValid(submittedTask.getTaskId())) {
            throw new BizException("task.rebalance.invalidPreview");
        }
        TaskDto task = taskService.findByTaskId(new ObjectId(submittedTask.getTaskId()), "_id", "name", "status", "agentId", "type", "syncType", "accessNodeType", "accessNodeProcessIdList", "milestones", "currentEventTimestamp", "snapshotDoneAt", "dag", "startTime");
        if (task == null) {
            throw new BizException("task.rebalance.invalidPreview");
        }
        TaskRebalancePreviewVo.TaskPreview currentTask = ruleService.evaluate(task, onlineAgentIds);
        if (!Boolean.TRUE.equals(currentTask.getMovable())
                || !Objects.equals(currentTask.getSourceAgentId(), submittedTask.getSourceAgentId())) {
            throw new BizException("task.rebalance.invalidPreview");
        }
    }

    public Page<TaskRebalanceVo> findHistory(Filter filter, UserDetail userDetail) {
        assertRebalanceEnabled();
        assertRebalancePermission(userDetail);
        if (filter == null) {
            filter = new Filter();
        }
        Page<TaskRebalanceDto> page = find(filter, userDetail);
        List<TaskRebalanceVo> items = page.getItems() == null ? new ArrayList<>() : page.getItems().stream()
                .map(this::toVo)
                .collect(Collectors.toList());
        return Page.page(items, page.getTotal());
    }

    public TaskRebalanceDetailVo detail(String id, UserDetail userDetail) {
        assertRebalanceEnabled();
        assertRebalancePermission(userDetail);
        TaskRebalanceDetailVo detail = new TaskRebalanceDetailVo();
        detail.setRebalance(toVo(findById(new ObjectId(id), userDetail)));
        Query query = Query.query(Criteria.where(TaskRebalanceJobDto.FIELD_REBALANCE_ID).is(id))
                .with(Sort.by(Sort.Direction.ASC, TaskRebalanceJobDto.FIELD_CREATE_TIME));
        detail.setJobs(jobService.findAllDto(query, userDetail).stream()
                .map(this::toVo)
                .collect(Collectors.toList()));
        return detail;
    }

    public boolean hasActive(UserDetail userDetail) {
        assertRebalanceEnabled();
        assertRebalancePermission(userDetail);
        Query query = Query.query(Criteria.where(TaskRebalanceDto.FIELD_STATUS).is(TaskRebalanceStatus.RUNNING));
        return count(query, userDetail) > 0;
    }

    public void cancel(String rebalanceId, UserDetail userDetail) {
        assertRebalanceEnabled();
        assertRebalancePermission(userDetail);
        String cancelReason = buildCancelReason(userDetail);
        Query query = Query.query(Criteria.where(TaskRebalanceJobDto.FIELD_REBALANCE_ID).is(rebalanceId)
                .and(TaskRebalanceJobDto.FIELD_STATUS).is(TaskRebalanceJobStatus.PENDING));
        UpdateResult result = cancelPendingJobs(query, cancelReason, userDetail);
        if (result.getModifiedCount() == 0 && hasRunningJob(rebalanceId, null, userDetail)) {
            throw new BizException("task.rebalance.jobCannotCancel", rebalanceId);
        }
        updateProgress(rebalanceId, null, userDetail);
    }

    public void cancelJob(String rebalanceId, String taskId, UserDetail userDetail) {
        assertRebalanceEnabled();
        assertRebalancePermission(userDetail);
        TaskRebalanceJobDto job = findRebalanceJob(rebalanceId, taskId, userDetail);
        if (job == null) {
            throw new BizException("task.rebalance.jobNotFound", taskId);
        }
        if (!TaskRebalanceJobStatus.PENDING.equals(job.getStatus())) {
            throw new BizException("task.rebalance.jobCannotCancel", job.getStatus());
        }
        String cancelReason = buildCancelReason(userDetail);
        Query query = Query.query(Criteria.where(TaskRebalanceJobDto.FIELD_REBALANCE_ID).is(rebalanceId)
                .and(TaskRebalanceJobDto.FIELD_TASK_ID).is(taskId)
                .and(TaskRebalanceJobDto.FIELD_STATUS).is(TaskRebalanceJobStatus.PENDING));
        UpdateResult result = cancelPendingJobs(query, cancelReason, userDetail);
        if (result.getModifiedCount() == 0) {
            throw new BizException("task.rebalance.jobCannotCancel", taskId);
        }
        updateProgress(rebalanceId, null, userDetail);
    }

    private UpdateResult cancelPendingJobs(Query query, String cancelReason, UserDetail userDetail) {
        Update update = Update.update(TaskRebalanceJobDto.FIELD_STATUS, TaskRebalanceJobStatus.CANCELLED)
                .set(TaskRebalanceJobDto.FIELD_FINISH_AT, new Date())
                .set(TaskRebalanceJobDto.FIELD_ERROR_MESG, cancelReason);
        return jobService.update(query, update, userDetail);
    }

    private TaskRebalanceJobDto findRebalanceJob(String rebalanceId, String taskId, UserDetail userDetail) {
        Query query = Query.query(Criteria.where(TaskRebalanceJobDto.FIELD_REBALANCE_ID).is(rebalanceId)
                .and(TaskRebalanceJobDto.FIELD_TASK_ID).is(taskId));
        return jobService.findOne(query, userDetail);
    }

    private boolean hasRunningJob(String rebalanceId, String taskId, UserDetail userDetail) {
        Criteria criteria = Criteria.where(TaskRebalanceJobDto.FIELD_REBALANCE_ID).is(rebalanceId)
                .and(TaskRebalanceJobDto.FIELD_STATUS).in(TaskRebalanceJobStatus.STOPPING, TaskRebalanceJobStatus.STARTING);
        if (StringUtils.isNotBlank(taskId)) {
            criteria.and(TaskRebalanceJobDto.FIELD_TASK_ID).is(taskId);
        }
        return jobService.count(Query.query(criteria), userDetail) > 0;
    }

    static String buildCancelReason(UserDetail userDetail) {
        String username = userDetail == null ? null : userDetail.getUsername();
        return "Cancelled by " + StringUtils.defaultIfBlank(username, "user");
    }

    /**
     * Runs one scheduler tick on the active TM node, selecting the oldest running rebalance
     * and executing it with the rebalance creator's user context.
     */
    public void scheduleOnce() {
        if (settingsService.isCloud()) {
            return;
        }
        Query query = Query.query(Criteria.where(TaskRebalanceDto.FIELD_STATUS).is(TaskRebalanceStatus.RUNNING))
                .with(Sort.by(Sort.Direction.ASC, TaskRebalanceDto.FIELD_CREATE_TIME));
        List<TaskRebalanceDto> rebalances = findAll(query);
        if (CollectionUtils.isEmpty(rebalances)) {
            return;
        }
        TaskRebalanceDto rebalance = rebalances.get(0);
        UserDetail user;
        try {
            user = loadRebalanceUser(rebalance);
        } catch (Exception e) {
            String reason = "Load rebalance user failed: " + e.getMessage();
            log.warn("TaskRebalance load user failed, rebalanceId={}", rebalance.getId(), e);
            failScheduledRebalance(rebalance, reason);
            return;
        }
        try {
            execute(rebalance.getId().toHexString(), user);
        } catch (Exception e) {
            log.warn("TaskRebalance schedule failed, rebalanceId={}", rebalance.getId(), e);
        }
    }

    private UserDetail loadRebalanceUser(TaskRebalanceDto rebalance) {
        String userId = rebalance == null ? null : rebalance.getUserId();
        if (!ObjectId.isValid(userId)) {
            throw new IllegalArgumentException("Invalid rebalance userId " + userId);
        }
        UserDetail user = userService.loadUserById(new ObjectId(userId));
        if (user == null) {
            throw new IllegalStateException("Rebalance user not found " + userId);
        }
        return user;
    }

    private void failScheduledRebalance(TaskRebalanceDto rebalance, String reason) {
        if (rebalance == null || rebalance.getId() == null) {
            return;
        }
        Update update = Update.update(TaskRebalanceDto.FIELD_STATUS, TaskRebalanceStatus.FAILED)
                .set(TaskRebalanceDto.FIELD_FINISH_AT, new Date())
                .set(TaskRebalanceDto.FIELD_ERROR_MESG, reason);
        updateById(rebalance.getId(), update, null);
    }

    private void assertRebalanceEnabled() {
        if (settingsService.isCloud()) {
            throw new BizException("task.rebalance.disabled");
        }
    }

    private void assertRebalancePermission(UserDetail userDetail) {
        if (userDetail.isRoot() || userDetail.isFreeAuth()) {
            return;
        }
        DataPermissionHelper.check(
                userDetail,
                DataPermissionMenuEnums.TaskRebalance,
                DataPermissionActionEnums.View,
                DataPermissionDataTypeEnums.Task,
                null,
                () -> true,
                () -> {
                    throw new BizException("insufficient.permissions", "task.view", "task.view");
                });
    }

    /**
     * Executes active jobs for one rebalance. Jobs with the same target agent run serially,
     * while different target agents are processed concurrently by a bounded executor.
     * If any worker detects a target agent going offline, the rebalance is aborted:
     * already in-flight jobs finish their stop/start cycle, while remaining pending jobs
     * are cancelled with an abort reason.
     */
    public void execute(String rebalanceId, UserDetail userDetail) {
        Query query = Query.query(Criteria.where(TaskRebalanceJobDto.FIELD_REBALANCE_ID).is(rebalanceId)
                .and(TaskRebalanceJobDto.FIELD_STATUS).in(TaskRebalanceJobStatus.PENDING, TaskRebalanceJobStatus.STOPPING, TaskRebalanceJobStatus.STARTING));
        List<TaskRebalanceJobDto> jobs = jobService.findAllDto(query, userDetail);
        if (CollectionUtils.isEmpty(jobs)) {
            updateProgress(rebalanceId, null, userDetail);
            return;
        }
        Map<String, List<TaskRebalanceJobDto>> jobsByTargetAgent = jobs.stream()
                .collect(Collectors.groupingBy(job -> StringUtils.defaultIfBlank(job.getTargetAgentId(), "")));
        int threadCount = Math.min(jobsByTargetAgent.size(), MAX_TARGET_AGENT_CONCURRENCY);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount, newNamedThreadFactory(rebalanceId));
        AtomicBoolean abortFlag = new AtomicBoolean(false);
        AtomicReference<String> abortReason = new AtomicReference<>();
        CompletableFuture<?>[] futures = jobsByTargetAgent.values().stream()
                .map(targetJobs -> CompletableFuture.runAsync(
                        () -> executeTargetAgentJobs(rebalanceId, targetJobs, userDetail, abortFlag, abortReason),
                        executor))
                .toArray(CompletableFuture[]::new);
        try {
            waitFutures(futures);
        } finally {
            shutdownExecutor(executor, rebalanceId);
        }
        String finalErrorMesg = abortFlag.get() ? abortReason.get() : null;
        if (abortFlag.get()) {
            abortPendingJobs(rebalanceId, finalErrorMesg, userDetail);
        }
        updateProgress(rebalanceId, finalErrorMesg, userDetail);
    }

    private void executeTargetAgentJobs(String rebalanceId, List<TaskRebalanceJobDto> jobs, UserDetail userDetail,
                                        AtomicBoolean abortFlag, AtomicReference<String> abortReason) {
        for (TaskRebalanceJobDto job : jobs) {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            if (abortFlag.get()) {
                cancelOnAbort(job, abortReason.get(), userDetail);
                updateProgress(rebalanceId, abortReason.get(), userDetail);
                continue;
            }
            executeJob(job, userDetail, abortFlag, abortReason);
            updateProgress(rebalanceId, abortFlag.get() ? abortReason.get() : null, userDetail);
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
        }
    }

    /**
     * Executes one task migration by revalidating runtime state, stopping the task,
     * updating its agent id, starting it again, and mapping every terminal path to a job status.
     * Detecting that the target agent has gone offline raises the shared abort flag so that
     * the rest of the rebalance is short-circuited; jobs that already entered stop/start finish on their own.
     */
    private void executeJob(TaskRebalanceJobDto job, UserDetail userDetail,
                            AtomicBoolean abortFlag, AtomicReference<String> abortReason) {
        TaskRebalanceJobDto latest = jobService.findById(job.getId(), userDetail);
        if (latest == null || !TaskRebalanceJobStatus.ACTIVE_STATUS.contains(latest.getStatus())) {
            return;
        }
        try {
            if (TaskRebalanceJobStatus.PENDING.equals(latest.getStatus())) {
                executePendingJob(latest, userDetail, abortFlag, abortReason);
            } else if (TaskRebalanceJobStatus.STOPPING.equals(latest.getStatus())) {
                resumeStoppingJob(latest, userDetail, abortFlag, abortReason);
            } else if (TaskRebalanceJobStatus.STARTING.equals(latest.getStatus())) {
                resumeStartingJob(latest, userDetail, abortFlag, abortReason);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("TaskRebalance job interrupted, jobId={}, taskId={}", latest.getId(), latest.getTaskId());
        } catch (Exception e) {
            log.warn("TaskRebalance job failed, jobId={}, taskId={}, error={}", latest.getId(), latest.getTaskId(), e.getMessage(), e);
            String reason = e.getMessage();
            finishJob(latest, TaskRebalanceJobStatus.STATUS_ERROR, reason, userDetail);
        }
    }

    private void executePendingJob(TaskRebalanceJobDto job, UserDetail userDetail,
                                   AtomicBoolean abortFlag, AtomicReference<String> abortReason) throws InterruptedException {
        if (!isAgentOnline(job.getTargetAgentId())) {
            String reason = formatTargetOfflineReason(job, "before stop");
            finishJob(job, TaskRebalanceJobStatus.INVALID_AGENT, reason, userDetail);
            raiseAbort(abortFlag, abortReason, reason);
            return;
        }
        TaskDto task = findTaskForExecution(job.getTaskId());
        if (!ruleService.isMovableAtExecution(task)) {
            finishJob(job, TaskRebalanceJobStatus.STATUS_ERROR, "Task status changed or not movable", userDetail);
            return;
        }
        UserDetail taskUser = getTaskUser(task, userDetail);
        if (!Objects.equals(task.getAgentId(), job.getSourceAgentId())) {
            finishJob(job, TaskRebalanceJobStatus.STATUS_ERROR, "Task source agent changed", userDetail);
            return;
        }
        if (Objects.equals(task.getAgentId(), job.getTargetAgentId())) {
            finishJob(job, TaskRebalanceJobStatus.STATUS_ERROR, "Task already on target agent", userDetail);
            return;
        }

        if (!transitionJobStatus(job, TaskRebalanceJobStatus.PENDING, TaskRebalanceJobStatus.STOPPING, null, true, userDetail)) {
            log.info("TaskRebalance job transition skipped, jobId={}, taskId={}, from={}, to={}",
                    job.getId(), job.getTaskId(), TaskRebalanceJobStatus.PENDING, TaskRebalanceJobStatus.STOPPING);
            return;
        }
        stopSourceTask(job, task, taskUser, userDetail, abortFlag, abortReason);
    }

    private void resumeStoppingJob(TaskRebalanceJobDto job, UserDetail userDetail,
                                   AtomicBoolean abortFlag, AtomicReference<String> abortReason) throws InterruptedException {
        TaskDto task = findTaskForExecution(job.getTaskId());
        if (task == null) {
            finishJob(job, TaskRebalanceJobStatus.STATUS_ERROR, "Task no longer exists", userDetail);
            return;
        }
        if (TaskDto.STATUS_RUNNING.equals(task.getStatus()) && Objects.equals(task.getAgentId(), job.getTargetAgentId())) {
            finishJob(job, TaskRebalanceJobStatus.OK, null, userDetail);
            return;
        }
        UserDetail taskUser = getTaskUser(task, userDetail);
        if (Objects.equals(task.getAgentId(), job.getTargetAgentId())) {
            resumeStartingJob(job, userDetail, abortFlag, abortReason);
            return;
        }
        if (!Objects.equals(task.getAgentId(), job.getSourceAgentId())) {
            finishJob(job, TaskRebalanceJobStatus.STATUS_ERROR, "Task agent changed while stopping", userDetail);
            return;
        }
        if (!TaskDto.STATUS_STOP.equals(task.getStatus()) && !isAgentOnline(job.getTargetAgentId())) {
            String reason = formatTargetOfflineReason(job, "before stop");
            finishJob(job, TaskRebalanceJobStatus.INVALID_AGENT, reason, userDetail);
            raiseAbort(abortFlag, abortReason, reason);
            return;
        }
        stopSourceTask(job, task, taskUser, userDetail, abortFlag, abortReason);
    }

    private void stopSourceTask(TaskRebalanceJobDto job, TaskDto task, UserDetail taskUser, UserDetail userDetail,
                                AtomicBoolean abortFlag, AtomicReference<String> abortReason) throws InterruptedException {
        if (!TaskDto.STATUS_STOP.equals(task.getStatus())) {
            jobService.runAsRebalanceOperation(() -> taskService.pause(task, taskUser, false));
        }
        if (!waitTaskStatus(job.getTaskId(), TaskDto.STATUS_STOP, getTaskStatusTimeoutMs())) {
            String reason = formatStopTimeoutReason(job);
            finishJob(job, TaskRebalanceJobStatus.STOP_TIMEOUT, reason, userDetail);
            return;
        }
        moveStoppedTaskToTargetAndStart(job, taskUser, userDetail, abortFlag, abortReason);
    }

    private void moveStoppedTaskToTargetAndStart(TaskRebalanceJobDto job, UserDetail taskUser, UserDetail userDetail,
                                                 AtomicBoolean abortFlag, AtomicReference<String> abortReason) throws InterruptedException {
        if (!isAgentOnline(job.getTargetAgentId())) {
            String reason = formatTargetOfflineReason(job, "before start; task left stopped on source agent " + job.getSourceAgentId());
            finishJob(job, TaskRebalanceJobStatus.INVALID_AGENT, reason, userDetail);
            raiseAbort(abortFlag, abortReason, reason);
            return;
        }
        if (TaskRebalanceJobStatus.STOPPING.equals(job.getStatus())
                && !transitionJobStatus(job, TaskRebalanceJobStatus.STOPPING, TaskRebalanceJobStatus.STARTING, null, false, userDetail)) {
            log.info("TaskRebalance job transition skipped, jobId={}, taskId={}, from={}, to={}",
                    job.getId(), job.getTaskId(), TaskRebalanceJobStatus.STOPPING, TaskRebalanceJobStatus.STARTING);
            return;
        }
        taskService.updateById(new ObjectId(job.getTaskId()), Update.update("agentId", job.getTargetAgentId()), taskUser);
        startTargetTask(job, taskUser, userDetail, abortFlag, abortReason);
    }

    private void resumeStartingJob(TaskRebalanceJobDto job, UserDetail userDetail,
                                   AtomicBoolean abortFlag, AtomicReference<String> abortReason) throws InterruptedException {
        if (isTaskRunningOnTarget(job)) {
            finishJob(job, TaskRebalanceJobStatus.OK, null, userDetail);
            return;
        }
        TaskDto task = findTaskForExecution(job.getTaskId());
        if (task == null) {
            finishJob(job, TaskRebalanceJobStatus.STATUS_ERROR, "Task no longer exists", userDetail);
            return;
        }
        UserDetail taskUser = getTaskUser(task, userDetail);
        if (Objects.equals(task.getAgentId(), job.getSourceAgentId())) {
            if (!TaskDto.STATUS_STOP.equals(task.getStatus())) {
                jobService.runAsRebalanceOperation(() -> taskService.pause(task, taskUser, false));
                if (!waitTaskStatus(job.getTaskId(), TaskDto.STATUS_STOP, getTaskStatusTimeoutMs())) {
                    String reason = formatStopTimeoutReason(job);
                    finishJob(job, TaskRebalanceJobStatus.STOP_TIMEOUT, reason, userDetail);
                    return;
                }
            }
            moveStoppedTaskToTargetAndStart(job, taskUser, userDetail, abortFlag, abortReason);
            return;
        }
        if (!Objects.equals(task.getAgentId(), job.getTargetAgentId())) {
            finishJob(job, TaskRebalanceJobStatus.STATUS_ERROR, "Task agent changed while starting", userDetail);
            return;
        }
        if (!isAgentOnline(job.getTargetAgentId())) {
            String reason = formatTargetOfflineReason(job, "during start");
            String rollbackMessage = rollbackToSourceAgent(job, taskUser);
            if (StringUtils.isNotBlank(rollbackMessage)) {
                reason = reason + "; " + rollbackMessage;
            }
            finishJob(job, TaskRebalanceJobStatus.INVALID_AGENT, reason, userDetail);
            raiseAbort(abortFlag, abortReason, reason);
            return;
        }
        if (TaskDto.STATUS_STOP.equals(task.getStatus())) {
            startTargetTask(job, taskUser, userDetail, abortFlag, abortReason);
            return;
        }
        waitTargetRunningOrRollback(job, taskUser, userDetail, abortFlag, abortReason);
    }

    private void startTargetTask(TaskRebalanceJobDto job, UserDetail taskUser, UserDetail userDetail,
                                 AtomicBoolean abortFlag, AtomicReference<String> abortReason) throws InterruptedException {
        TaskDto stoppedTask = taskService.findByTaskId(new ObjectId(job.getTaskId()));
        try {
            jobService.runAsRebalanceOperation(() -> taskService.start(stoppedTask, taskUser, "11"));
        } catch (Exception startError) {
            handleStartFailure(job, taskUser, startError.getMessage(), userDetail, abortFlag, abortReason);
            return;
        }
        waitTargetRunningOrRollback(job, taskUser, userDetail, abortFlag, abortReason);
    }

    private void waitTargetRunningOrRollback(TaskRebalanceJobDto job, UserDetail taskUser, UserDetail userDetail,
                                             AtomicBoolean abortFlag, AtomicReference<String> abortReason) throws InterruptedException {
        if (!waitTaskStatus(job.getTaskId(), TaskDto.STATUS_RUNNING, getTaskStatusTimeoutMs())) {
            if (isTaskRunningOnTarget(job)) {
                finishJob(job, TaskRebalanceJobStatus.OK, null, userDetail);
                return;
            }
            String reason = formatStartTimeoutReason(job);
            String rollbackMessage = rollbackToSourceAgent(job, taskUser);
            if (StringUtils.isNotBlank(rollbackMessage)) {
                reason = reason + "; " + rollbackMessage;
            }
            finishJob(job, TaskRebalanceJobStatus.START_TIMEOUT, reason, userDetail);
            return;
        }
        finishJob(job, TaskRebalanceJobStatus.OK, null, userDetail);
    }

    private TaskDto findTaskForExecution(String taskId) {
        return taskService.findByTaskId(new ObjectId(taskId), "_id", "user_id", "name", "status", "agentId", "type", "milestones", "currentEventTimestamp", "snapshotDoneAt", "accessNodeType", "accessNodeProcessIdList", "syncType");
    }

    private void handleStartFailure(TaskRebalanceJobDto job, UserDetail taskUser, String errorMessage, UserDetail userDetail,
                                    AtomicBoolean abortFlag, AtomicReference<String> abortReason) {
        if (isTaskRunningOnTarget(job)) {
            finishJob(job, TaskRebalanceJobStatus.OK, null, userDetail);
            return;
        }
        String reason = MessageFormat.format("Task start command failed on target agent {0}: {1}", job.getTargetAgentId(), errorMessage);
        String rollbackMessage = rollbackToSourceAgent(job, taskUser);
        if (StringUtils.isNotBlank(rollbackMessage)) {
            reason = reason + "; " + rollbackMessage;
        }
        finishJob(job, TaskRebalanceJobStatus.START_TIMEOUT, reason, userDetail);
    }

    private boolean isTaskRunningOnTarget(TaskRebalanceJobDto job) {
        TaskDto task = taskService.findByTaskId(new ObjectId(job.getTaskId()), "status", "agentId");
        return task != null
                && TaskDto.STATUS_RUNNING.equals(task.getStatus())
                && Objects.equals(task.getAgentId(), job.getTargetAgentId());
    }

    private String rollbackToSourceAgent(TaskRebalanceJobDto job, UserDetail taskUser) {
        TaskDto current = taskService.findByTaskId(new ObjectId(job.getTaskId()), "_id", "name", "status", "agentId");
        if (current == null) {
            return "rollback skipped because task no longer exists";
        }
        if (Objects.equals(current.getAgentId(), job.getSourceAgentId())) {
            return "agentId already on source agent " + job.getSourceAgentId();
        }
        if (!Objects.equals(current.getAgentId(), job.getTargetAgentId())) {
            return MessageFormat.format("rollback skipped because task agent changed to {0}", current.getAgentId());
        }
        if (TaskDto.STATUS_RUNNING.equals(current.getStatus())) {
            return MessageFormat.format("rollback skipped because task is already running on target agent {0}", job.getTargetAgentId());
        }
        try {
            if (!TaskDto.STATUS_STOP.equals(current.getStatus())) {
                jobService.runAsRebalanceOperation(() -> taskService.pause(current, taskUser, false));
                waitTaskStatus(job.getTaskId(), TaskDto.STATUS_STOP, getTaskStatusTimeoutMs());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("TaskRebalance rollback interrupted, jobId={}, taskId={}", job.getId(), job.getTaskId(), e);
        } catch (Exception e) {
            log.warn("TaskRebalance rollback stop failed, jobId={}, taskId={}, error={}", job.getId(), job.getTaskId(), e.getMessage(), e);
        }
        TaskDto latest = taskService.findByTaskId(new ObjectId(job.getTaskId()), "status", "agentId");
        if (latest != null
                && Objects.equals(latest.getAgentId(), job.getTargetAgentId())
                && !TaskDto.STATUS_RUNNING.equals(latest.getStatus())) {
            taskService.updateById(new ObjectId(job.getTaskId()), Update.update("agentId", job.getSourceAgentId()), taskUser);
            return MessageFormat.format("agentId rolled back from target agent {0} to source agent {1}", job.getTargetAgentId(), job.getSourceAgentId());
        }
        return "rollback skipped because task state changed while recovering";
    }

    private void raiseAbort(AtomicBoolean abortFlag, AtomicReference<String> abortReason, String reason) {
        abortReason.compareAndSet(null, reason);
        abortFlag.set(true);
    }

    private String formatTargetOfflineReason(TaskRebalanceJobDto job, String phase) {
        return MessageFormat.format("Target agent {0} offline {1}", job.getTargetAgentId(), phase);
    }

    private String formatStopTimeoutReason(TaskRebalanceJobDto job) {
        return MessageFormat.format("Task did not stop before timeout; task may still be in stopping/stopped state on source agent {0}",
                job.getSourceAgentId());
    }

    private String formatStartTimeoutReason(TaskRebalanceJobDto job) {
        return MessageFormat.format("Task did not run on target agent {0} before timeout",
                job.getTargetAgentId());
    }

    /**
     * Marks a single still-pending job as cancelled when the rebalance is aborted by a peer worker.
     */
    private void cancelOnAbort(TaskRebalanceJobDto job, String reason, UserDetail userDetail) {
        TaskRebalanceJobDto latest = jobService.findById(job.getId(), userDetail);
        if (latest == null || !TaskRebalanceJobStatus.PENDING.equals(latest.getStatus())) {
            return;
        }
        finishJob(latest, TaskRebalanceJobStatus.CANCELLED, reason, userDetail);
    }

    /**
     * Cleans up any pending jobs left after worker threads exit when the rebalance has been aborted.
     */
    private void abortPendingJobs(String rebalanceId, String reason, UserDetail userDetail) {
        Query query = Query.query(Criteria.where(TaskRebalanceJobDto.FIELD_REBALANCE_ID).is(rebalanceId)
                .and(TaskRebalanceJobDto.FIELD_STATUS).is(TaskRebalanceJobStatus.PENDING));
        Update update = Update.update(TaskRebalanceJobDto.FIELD_STATUS, TaskRebalanceJobStatus.CANCELLED)
                .set(TaskRebalanceJobDto.FIELD_FINISH_AT, new Date())
                .set(TaskRebalanceJobDto.FIELD_ERROR_MESG, reason);
        jobService.update(query, update, userDetail);
    }

    private boolean transitionJobStatus(TaskRebalanceJobDto job, String expectedStatus, String status, String error, boolean begin, UserDetail userDetail) {
        Query query = Query.query(Criteria.where("_id").is(job.getId())
                .and(TaskRebalanceJobDto.FIELD_STATUS).is(expectedStatus));
        Update update = Update.update(TaskRebalanceJobDto.FIELD_STATUS, status);
        if (begin && job.getBeginAt() == null) {
            update.set(TaskRebalanceJobDto.FIELD_BEGIN_AT, new Date());
        }
        if (StringUtils.isNotBlank(error)) {
            update.set(TaskRebalanceJobDto.FIELD_ERROR_MESG, error);
        }
        return jobService.update(query, update, userDetail).getModifiedCount() > 0;
    }

    private void finishJob(TaskRebalanceJobDto job, String status, String error, UserDetail userDetail) {
        Update update = Update.update(TaskRebalanceJobDto.FIELD_STATUS, status).set(TaskRebalanceJobDto.FIELD_FINISH_AT, new Date());
        if (StringUtils.isNotBlank(error)) {
            update.set(TaskRebalanceJobDto.FIELD_ERROR_MESG, error);
        }
        if (job.getBeginAt() == null) {
            update.set(TaskRebalanceJobDto.FIELD_BEGIN_AT, new Date());
        }
        jobService.updateById(job.getId(), update, userDetail);
    }

    private long getTaskStatusTimeoutMs() {
        try {
            return Long.parseLong(SettingsEnum.JOB_HEART_TIMEOUT.getValue(String.valueOf(DEFAULT_TASK_STATUS_TIMEOUT_MS)));
        } catch (Exception e) {
            log.warn("TaskRebalance read jobHeartTimeout failed, using default {}ms", DEFAULT_TASK_STATUS_TIMEOUT_MS, e);
            return DEFAULT_TASK_STATUS_TIMEOUT_MS;
        }
    }

    private boolean waitTaskStatus(String taskId, String expectedStatus, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Task rebalance execution interrupted");
            }
            TaskDto task = taskService.findByTaskId(new ObjectId(taskId), "status");
            if (task != null && expectedStatus.equals(task.getStatus())) {
                return true;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        return false;
    }

    /**
     * Waits for all target-agent worker futures while periodically checking interruptions,
     * cancelling unfinished futures when DBLock standby or executor interruption occurs.
     */
    private void waitFutures(CompletableFuture<?>[] futures) {
        CompletableFuture<Void> all = CompletableFuture.allOf(futures);
        while (!all.isDone()) {
            if (Thread.currentThread().isInterrupted()) {
                cancelFutures(futures);
                return;
            }
            try {
                all.get(EXECUTION_WAIT_INTERVAL_MS, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ignored) {
            } catch (InterruptedException e) {
                cancelFutures(futures);
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException e) {
                log.warn("TaskRebalance target agent execution failed", e.getCause());
                return;
            }
        }
    }

    private void cancelFutures(CompletableFuture<?>[] futures) {
        for (CompletableFuture<?> future : futures) {
            future.cancel(true);
        }
    }

    private void shutdownExecutor(ExecutorService executor, String rebalanceId) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                log.warn("TaskRebalance executor did not terminate in time, rebalanceId={}", rebalanceId);
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            log.warn("TaskRebalance executor shutdown interrupted, rebalanceId={}", rebalanceId);
        }
    }

    private ThreadFactory newNamedThreadFactory(String rebalanceId) {
        AtomicInteger index = new AtomicInteger(0);
        return runnable -> {
            Thread thread = new Thread(runnable, "TaskRebalance-" + rebalanceId + "-" + index.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private boolean isAgentOnline(String agentId) {
        if (StringUtils.isBlank(agentId)) {
            return false;
        }
        List<Worker> workers = findAvailableAgents();
        if (CollectionUtils.isEmpty(workers)) {
            return false;
        }
        return workers.stream().anyMatch(worker -> agentId.equals(worker.getProcessId()));
    }

    private List<Worker> findAvailableAgents() {
        int overTime = SettingsEnum.WORKER_HEART_OVERTIME.getIntValue(30);
        Query query = Query.query(Criteria.where("worker_type").is("connector")
                .and("ping_time").gte(System.currentTimeMillis() - (overTime * 1000L) + 1L)
                .and("isDeleted").ne(true)
                .and("stopping").ne(true)
                .and("agentTags").ne("disabledScheduleTask"));
        return workerService.findAllEntity(query);
    }

    private UserDetail getTaskUser(TaskDto task, UserDetail fallback) {
        if (StringUtils.isBlank(task.getUserId())) {
            return fallback;
        }
        try {
            return userService.loadUserById(new ObjectId(task.getUserId()));
        } catch (Exception e) {
            log.warn("TaskRebalance load task user failed, taskId={}, userId={}", task.getId(), task.getUserId(), e);
            return fallback;
        }
    }

    /**
     * Rebuilds parent progress counters from all child jobs and marks the rebalance finished
     * when there are no pending, stopping, or starting jobs left. When an abort reason is provided,
     * it is recorded on the rebalance so operators can see why the rebalance terminated.
     */
    private void updateProgress(String rebalanceId, String errorMesg, UserDetail userDetail) {
        Query query = Query.query(Criteria.where(TaskRebalanceJobDto.FIELD_REBALANCE_ID).is(rebalanceId));
        List<TaskRebalanceJobDto> jobs = jobService.findAllDto(query, userDetail);
        int pending = 0;
        int stopping = 0;
        int starting = 0;
        int ok = 0;
        int cancelled = 0;
        int failed = 0;
        for (TaskRebalanceJobDto job : jobs) {
            String status = job.getStatus();
            if (TaskRebalanceJobStatus.PENDING.equals(status)) {
                pending++;
            } else if (TaskRebalanceJobStatus.STOPPING.equals(status)) {
                stopping++;
            } else if (TaskRebalanceJobStatus.STARTING.equals(status)) {
                starting++;
            } else if (TaskRebalanceJobStatus.OK.equals(status)) {
                ok++;
            } else if (TaskRebalanceJobStatus.CANCELLED.equals(status)) {
                cancelled++;
            } else if (TaskRebalanceJobStatus.isTerminal(status)) {
                failed++;
            }
        }
        int active = pending + stopping + starting;
        Update update = Update.update(TaskRebalanceDto.FIELD_TOTAL_COUNT, jobs.size())
                .set(TaskRebalanceDto.FIELD_PENDING_COUNT, pending)
                .set(TaskRebalanceDto.FIELD_STOPPING_COUNT, stopping)
                .set(TaskRebalanceDto.FIELD_STARTING_COUNT, starting)
                .set(TaskRebalanceDto.FIELD_OK_COUNT, ok)
                .set(TaskRebalanceDto.FIELD_FAILED_COUNT, failed)
                .set(TaskRebalanceDto.FIELD_CANCELLED_COUNT, cancelled);
        if (StringUtils.isNotBlank(errorMesg)) {
            update.set(TaskRebalanceDto.FIELD_ERROR_MESG, errorMesg);
        }
        if (active == 0) {
            update.set(TaskRebalanceDto.FIELD_FINISH_AT, new Date());
            update.set(TaskRebalanceDto.FIELD_STATUS, TaskRebalanceStatus.finalStatus(failed, cancelled));
        }
        updateById(new ObjectId(rebalanceId), update, userDetail);
    }

    private List<TaskDto> findStatTasks() {
        Query query = Query.query(Criteria.where("is_deleted").ne(true)
                .and("status").in(TaskDto.STATUS_RUNNING, TaskDto.STATUS_SCHEDULING, TaskDto.STATUS_WAIT_RUN));
        query.fields().include("_id", "name", "status", "agentId", "type", "syncType", "accessNodeType", "accessNodeProcessIdList", "milestones", "currentEventTimestamp", "snapshotDoneAt", "dag", "startTime");
        return taskService.findAll(query);
    }

    /**
     * Builds the preview by counting current task distribution, evaluating movability rules in memory,
     * and greedily assigning high-priority movable tasks from overloaded agents to underloaded agents.
     */
    private TaskRebalancePreviewVo buildPreview(List<TaskDto> tasks, Map<String, Worker> workerMap) {
        TaskRebalancePreviewVo preview = new TaskRebalancePreviewVo();
        Map<String, Integer> afterCount = initCountMap(workerMap.keySet());
        for (TaskDto task : tasks) {
            if (StringUtils.isNotBlank(task.getAgentId()) && afterCount.containsKey(task.getAgentId())) {
                afterCount.put(task.getAgentId(), afterCount.get(task.getAgentId()) + 1);
            }
        }

        int total = afterCount.values().stream().mapToInt(Integer::intValue).sum();
        List<String> agents = new ArrayList<>(workerMap.keySet());
        agents.sort(String::compareTo);
        Map<String, Integer> targetCount = targetCount(total, agents);
        Map<String, List<TaskRebalancePreviewVo.TaskPreview>> movableByAgent = new HashMap<>();

        for (TaskDto task : tasks) {
            TaskRebalancePreviewVo.TaskPreview item = ruleService.evaluate(task, workerMap.keySet());
            if (Boolean.TRUE.equals(item.getMovable())) {
                movableByAgent.computeIfAbsent(task.getAgentId(), k -> new ArrayList<>()).add(item);
            }
            preview.getTasks().add(item);
        }
        movableByAgent.values().forEach(list -> list.sort(ruleService::compareMovePriority));

        while (true) {
            String source = findOverAgent(afterCount, targetCount);
            String target = findUnderAgent(afterCount, targetCount);
            if (source == null || target == null) {
                break;
            }
            List<TaskRebalancePreviewVo.TaskPreview> candidates = movableByAgent.get(source);
            if (CollectionUtils.isEmpty(candidates)) {
                targetCount.put(source, afterCount.get(source));
                continue;
            }
            TaskRebalancePreviewVo.TaskPreview task = candidates.remove(0);
            task.setTargetAgentId(target);
            task.setChanged(true);
            afterCount.put(source, afterCount.get(source) - 1);
            afterCount.put(target, afterCount.get(target) + 1);
        }
        preview.setMoveCount((int) preview.getTasks().stream().filter(t -> Boolean.TRUE.equals(t.getChanged())).count());
        return preview;
    }

    private Map<String, Integer> initCountMap(Set<String> agentIds) {
        Map<String, Integer> countMap = new LinkedHashMap<>();
        for (String agentId : agentIds) {
            countMap.put(agentId, 0);
        }
        return countMap;
    }

    private Map<String, Integer> targetCount(int total, List<String> agents) {
        Map<String, Integer> target = new LinkedHashMap<>();
        int size = agents.size();
        int base = total / size;
        int mod = total % size;
        for (int i = 0; i < agents.size(); i++) {
            target.put(agents.get(i), base + (i < mod ? 1 : 0));
        }
        return target;
    }

    private String findOverAgent(Map<String, Integer> current, Map<String, Integer> target) {
        return current.keySet().stream()
                .filter(agent -> current.get(agent) > target.get(agent))
                .max(Comparator.comparingInt(current::get))
                .orElse(null);
    }

    private String findUnderAgent(Map<String, Integer> current, Map<String, Integer> target) {
        return current.keySet().stream()
                .filter(agent -> current.get(agent) < target.get(agent))
                .min(Comparator.comparingInt(current::get))
                .orElse(null);
    }

    private TaskRebalanceVo toVo(TaskRebalanceDto dto) {
        if (dto == null) {
            return null;
        }
        TaskRebalanceVo vo = new TaskRebalanceVo();
        vo.setId(dto.getId() == null ? null : dto.getId().toHexString());
        vo.setCustomId(dto.getCustomId());
        vo.setCreateTime(dto.getCreateAt());
        vo.setLastUpdated(dto.getLastUpdAt());
        vo.setUserId(dto.getUserId());
        vo.setLastUpdBy(dto.getLastUpdBy());
        vo.setCreateUser(dto.getCreateUser());
        vo.setName(dto.getName());
        vo.setStatus(dto.getStatus());
        vo.setFinishAt(dto.getFinishAt());
        vo.setTotalCount(dto.getTotalCount());
        vo.setPendingCount(dto.getPendingCount());
        vo.setStoppingCount(dto.getStoppingCount());
        vo.setStartingCount(dto.getStartingCount());
        vo.setOkCount(dto.getOkCount());
        vo.setFailedCount(dto.getFailedCount());
        vo.setCancelledCount(dto.getCancelledCount());
        vo.setErrorMesg(dto.getErrorMesg());
        return vo;
    }

    private TaskRebalanceJobVo toVo(TaskRebalanceJobDto dto) {
        if (dto == null) {
            return null;
        }
        TaskRebalanceJobVo vo = new TaskRebalanceJobVo();
        vo.setId(dto.getId() == null ? null : dto.getId().toHexString());
        vo.setCustomId(dto.getCustomId());
        vo.setCreateTime(dto.getCreateAt());
        vo.setLastUpdated(dto.getLastUpdAt());
        vo.setUserId(dto.getUserId());
        vo.setLastUpdBy(dto.getLastUpdBy());
        vo.setCreateUser(dto.getCreateUser());
        vo.setRebalanceId(dto.getRebalanceId());
        vo.setTaskId(dto.getTaskId());
        vo.setTaskName(dto.getTaskName());
        vo.setStatus(dto.getStatus());
        vo.setErrorMesg(dto.getErrorMesg());
        vo.setSourceAgentId(dto.getSourceAgentId());
        vo.setTargetAgentId(dto.getTargetAgentId());
        vo.setBeginAt(dto.getBeginAt());
        vo.setFinishAt(dto.getFinishAt());
        return vo;
    }
}
