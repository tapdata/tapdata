package com.tapdata.tm.worker.schedule;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.base.dto.SchedulableDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.scheduleTasks.dto.ScheduleTasksDto;
import com.tapdata.tm.scheduleTasks.service.ScheduleTasksService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.worker.dto.WorkSchedule;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.WorkerExpire;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public abstract class WorkerScheduler {
    protected MongoTemplate mongoTemplate;

    protected ScheduleTasksService scheduleTasksService;

    protected TaskService taskService;

    protected WorkerService workerService;

    protected SettingsService settingsService;


    public WorkerScheduler(ScheduleTasksService scheduleTasksService, TaskService taskService,
                           WorkerService workerService, SettingsService settingsService,
                           MongoTemplate mongoTemplate) {
        this.scheduleTasksService = scheduleTasksService;
        this.taskService = taskService;
        this.workerService = workerService;
        this.settingsService = settingsService;
        this.mongoTemplate = mongoTemplate;
    }

    /**
     * 1. returns an available worker.
     * 2. persist the selected worker to db.
     * @param entity
     * @param userDetail
     * @param type
     * @param name
     * @return
     */
    public CalculationEngineVo schedule(SchedulableDto entity, UserDetail userDetail, String type, String name) {
        CalculationEngineVo calculationEngineVo = selectAvailableWorker(entity, userDetail);
        saveWorker(userDetail, type, name, calculationEngineVo);
        return calculationEngineVo;
    }

    /**
     * returns an available worker.
     * @param entity
     * @param userDetail
     * @return
     */
    public CalculationEngineVo schedule(SchedulableDto entity, UserDetail userDetail) {
        return selectAvailableWorker(entity, userDetail);
    }

    private CalculationEngineVo selectAvailableWorker(SchedulableDto entity, UserDetail userDetail) {
        ArrayList<WorkSchedule> threadLog = new ArrayList<>();
        Object jobHeartTimeout = settingsService.getByCategoryAndKey(CategoryEnum.WORKER, KeyEnum.WORKER_HEART_TIMEOUT).getValue();
        boolean isCloud = settingsService.isCloud();
        if ((userDetail.getUserId() == null || userDetail.getUserId().equals("")) && isCloud) {
            throw new BizException("NotFoundUserId");
        }
        Long findTime = System.currentTimeMillis() - Long.parseLong((String) jobHeartTimeout) * 1000L;

        // 53迭代Task上增加了指定Flow Engine的功能 --start
        CalculationEngineVo calculationEngine = manuallyAssignAgent(entity, userDetail, findTime, threadLog);
        if (calculationEngine != null) return calculationEngine;
        // 53迭代Task上增加了指定Flow Engine的功能 --end
        return chooseOptimalWorker(entity, userDetail, findTime, isCloud, threadLog);
    }

    protected @NotNull CalculationEngineVo chooseOptimalWorker(SchedulableDto entity, UserDetail userDetail, Long findTime, boolean isCloud, ArrayList<WorkSchedule> threadLog) {
        CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
        AtomicReference<String> scheduleAgentId = new AtomicReference<>("");
        String filter;
        int availableNum;
        Criteria where = Criteria.where("worker_type").is("connector")
                .and("ping_time").gte(findTime)
                .and("isDeleted").ne(true)
                .and("stopping").ne(true);
        if (isCloud) {
            // query user have share worker
            WorkerExpire workerExpire = mongoTemplate.findOne(Query.query(Criteria.where("userId").is(userDetail.getUserId())), WorkerExpire.class);
            if (Objects.nonNull(workerExpire) && workerExpire.getExpireTime().after(new Date())) {
                where.and("user_id").in(userDetail.getUserId(), workerExpire.getShareTmUserId());
            } else {
                where.and("user_id").is(userDetail.getUserId());
            }
        }

        if (isCloud && entity.getAgentTags() != null && entity.getAgentTags().size() > 0) {
            List<String> agentTags = new ArrayList<>();
            for (int i = 0; i < entity.getAgentTags().size(); i++) {
                String s = entity.getAgentTags().get(i);
                if (!s.equals("unidirectional")) {
                    agentTags.add(s);
                }
            }
            if (CollectionUtils.isNotEmpty(agentTags)) {
                where.and("agentTags").all(agentTags);
            } else {
                where.and("agentTags").ne("disabledScheduleTask");
            }
        } else {
            where.and("agentTags").ne("disabledScheduleTask");
        }

        Query query = Query.query(where);
        List<WorkerDto> workers = workerService.findAll(query);
        if (CollectionUtils.isEmpty(workers)) {
            throw new BizException("Task.AgentNotFound");
        }
        availableNum = workers.size();

        AtomicInteger scheduleWeight = new AtomicInteger();
        AtomicInteger scheduleRunNum = new AtomicInteger();
        AtomicInteger scheduleTaskLimit = new AtomicInteger();
        AtomicInteger totalTaskLimit = new AtomicInteger();

        for (int i = 0; i < workers.size(); i++) {
            WorkerDto worker = workers.get(i);
            FunctionUtils.isTureOrFalse(worker.getUserId().equals(userDetail.getUserId())).trueOrFalseHandle(() -> worker.setWeight(99), () -> worker.setWeight(1));

            String processId = worker.getProcessId();
            int runningNum = taskService.runningTaskNum(processId, userDetail);
            int taskLimit = workerService.getLimitTaskNum(worker, userDetail);
            Integer weight = worker.getWeight();
            totalTaskLimit.addAndGet(taskLimit);
            if (isCloud && runningNum > taskLimit) {
                continue;
            }

            WorkSchedule workSchedule = new WorkSchedule();
            workSchedule.setProcessId(processId);
            workSchedule.setWeight(weight);
            workSchedule.setTaskRunNum(runningNum);
            workSchedule.setTaskLimit(taskLimit);
            threadLog.add(workSchedule);

            if (i == 0 || workSchedule.getProcessId() == null) {
                scheduleAgentId.set(processId);
                scheduleWeight.set(weight);
                scheduleRunNum.set(runningNum);
                scheduleTaskLimit.set(taskLimit);
            } else if (worker.getWeight() > scheduleWeight.get()) {
                scheduleAgentId.set(processId);
                scheduleWeight.set(weight);
                scheduleRunNum.set(runningNum);
                scheduleTaskLimit.set(taskLimit);
            } else if (worker.getWeight().equals(scheduleWeight.get()) && (taskLimit - runningNum) > (scheduleTaskLimit.get() - scheduleRunNum.get())) {
                scheduleAgentId.set(processId);
                scheduleWeight.set(weight);
                scheduleRunNum.set(runningNum);
                scheduleTaskLimit.set(taskLimit);
            }
        }
        int totalRunningNum = taskService.runningTaskNum(userDetail);
        filter = where.toString();
        String processId = scheduleAgentId.get();

        entity.setAgentId(processId);
        entity.setScheduleTime(System.currentTimeMillis());

        calculationEngineVo.setProcessId(processId);
        calculationEngineVo.setFilter(filter);
        calculationEngineVo.setThreadLog(threadLog);
        calculationEngineVo.setAvailable(availableNum);
        calculationEngineVo.setManually(false);
        int totalTask = totalTaskLimit.get() < 0 ? Integer.MAX_VALUE : totalTaskLimit.get();
        calculationEngineVo.setTaskLimit(totalTask);
        calculationEngineVo.setRunningNum(totalRunningNum);
        calculationEngineVo.setTotalLimit(totalTask);
        return calculationEngineVo;
    }

    private @Nullable CalculationEngineVo manuallyAssignAgent(SchedulableDto entity, UserDetail userDetail,
                                                              Long findTime, ArrayList<WorkSchedule> threadLog) {
        CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
        String agentId = entity.getAgentId();
        if (StringUtils.isNotBlank(agentId)) {
            Criteria where = Criteria.where("worker_type").is("connector")
                    .and("ping_time").gte(findTime)
                    .and("isDeleted").ne(true)
                    .and("stopping").ne(true)
                    .and("process_id").is(agentId);
            WorkerDto worker = workerService.findOne(Query.query(where));

            int runningNum = taskService.runningTaskNum(agentId, userDetail);
            if (Objects.nonNull(worker)) {
                int availableNum = 1;
                int taskLimit = workerService.getLimitTaskNum(worker, userDetail);

                calculationEngineVo.setProcessId(agentId);
                calculationEngineVo.setManually(true);

                calculationEngineVo.setFilter(where.toString());

                WorkSchedule workSchedule = new WorkSchedule();
                workSchedule.setProcessId(worker.getProcessId());
                workSchedule.setWeight(worker.getWeight());
                workSchedule.setTaskRunNum(runningNum);
                workSchedule.setTaskLimit(taskLimit);
                threadLog.add(workSchedule);
                calculationEngineVo.setThreadLog(threadLog);

                entity.setAgentId(calculationEngineVo.getProcessId());
                entity.setScheduleTime(System.currentTimeMillis());

                calculationEngineVo.setAvailable(availableNum);
                calculationEngineVo.setTaskLimit(taskLimit);
                calculationEngineVo.setRunningNum(runningNum);
                calculationEngineVo.setTotalLimit(taskLimit);
                return calculationEngineVo;
            }
        }
        return null;
    }

    private void saveWorker(UserDetail userDetail, String type, String name, CalculationEngineVo calculationEngineVo) {
        String processId = calculationEngineVo.getProcessId();
        String filter = calculationEngineVo.getFilter();

        ScheduleTasksDto scheduleTasksDto = new ScheduleTasksDto();
        scheduleTasksDto.setTask_name("TM_SCHEDULE");
        scheduleTasksDto.setType(type);
        scheduleTasksDto.setPeriod(0L);
        scheduleTasksDto.setStatus("done");
        scheduleTasksDto.setTask_name(name);
        scheduleTasksDto.setTask_profile("DEFAULT");
        scheduleTasksDto.setAgent_id(processId);
        scheduleTasksDto.setLast_updated(new Date());
        scheduleTasksDto.setPing_time(System.currentTimeMillis());
        scheduleTasksDto.setFilter(filter);
        scheduleTasksDto.setThread(calculationEngineVo.getThreadLog());
        scheduleTasksService.save(scheduleTasksDto, userDetail);
    }
}
