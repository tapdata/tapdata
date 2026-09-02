package com.tapdata.tm.dql.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.DqlRecoveryCallbackResultEnum;
import com.tapdata.tm.dql.DqlRecoveryAttemptResultEnum;
import com.tapdata.tm.dql.DqlRecoveryBatchStatusEnum;
import com.tapdata.tm.dql.DqlRecoveryReportTypeEnum;
import com.tapdata.tm.dql.config.DqlRuntimeConfig;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAuditEntryDto;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import com.tapdata.tm.dql.dto.DqlRecoveryNodeStateDto;
import com.tapdata.tm.dql.entity.DqlRecoveryTaskLockEntity;
import com.tapdata.tm.dql.repository.DqlEventRepository;
import com.tapdata.tm.dql.repository.DqlRecoveryBatchRepository;
import com.tapdata.tm.dql.repository.DqlRecoveryTaskLockRepository;
import com.tapdata.tm.dql.vo.DqlEventDetailVo;
import com.tapdata.tm.dql.vo.DqlRecoveryPreviewVo;
import com.tapdata.tm.dql.vo.DqlRecoveryRequestVo;
import com.tapdata.tm.dql.vo.DqlRecoveryResultReportVo;
import com.tapdata.tm.messagequeue.service.MessageQueueServiceImpl;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
public class DqlRecoveryBatchService {
    private static final int DEFAULT_BATCH_MAX_SIZE = DqlRuntimeConfig.DEFAULT_RECOVERY_BATCH_MAX_SIZE;
    private static final long DEFAULT_BATCH_TIMEOUT_MILLIS =
            DqlRuntimeConfig.DEFAULT_RECOVERY_BATCH_TIMEOUT_SECONDS * 1000L;
    private static final String RECOVERY_BATCH_TIMEOUT_MESSAGE = "Recovery batch timed out";
    private static final String RECOVERY_DISPATCH_TIMEOUT_MESSAGE =
            "DQL reprocessing timed out before Engine accepted the recovery batch";
    private static final String RECOVERY_HEARTBEAT_TIMEOUT_MESSAGE =
            "DQL reprocessing timed out because the Engine heartbeat expired";
    private static final String AUDIT_BATCH_CREATED = "BATCH_CREATED";
    private static final String AUDIT_BATCH_DISPATCHED = "BATCH_DISPATCHED";
    private static final String AUDIT_BATCH_STARTED = "BATCH_STARTED";
    private static final String AUDIT_EVENT_STARTED = "EVENT_STARTED";
    private static final String AUDIT_EVENT_RESULT = "EVENT_RESULT";
    private static final String AUDIT_BATCH_FINISHED = "BATCH_FINISHED";
    private static final String AUDIT_BATCH_FAILED = "BATCH_FAILED";
    private static final String AUDIT_BATCH_TIMEOUT = "BATCH_TIMEOUT";
    private static final String AUDIT_SOURCE_READ_PAUSE = "SOURCE_READ_PAUSE";
    private static final String AUDIT_SOURCE_READ_RESUME = "SOURCE_READ_RESUME";
    private static final String PREVIEW_SUMMARY_MESSAGE = "DqlRecovery.Preview.Summary";
    private static final String PREVIEW_EVENT_NOT_FOUND_MESSAGE = "DqlRecovery.Preview.EventNotFound";
    private static final String PREVIEW_EVENT_NO_BUSINESS_KEY_MESSAGE = "DqlRecovery.Preview.EventNoBusinessKey";
    private static final String PREVIEW_BATCH_SIZE_EXCEEDED_MESSAGE = "DqlRecovery.Preview.BatchSizeExceeded";
    private static final String PREVIEW_ACTIVE_BATCH_EXISTS_MESSAGE = "DqlRecovery.Preview.ActiveBatchExists";
    private static final String PREVIEW_STATUS_NOT_REPROCESSABLE_MESSAGE = "DqlRecovery.Preview.StatusNotReprocessable";
    private static final String PREVIEW_EVENT_NOT_REPROCESSABLE_MESSAGE = "DqlRecovery.Preview.EventNotReprocessable";
    private static final String PREVIEW_PAYLOAD_INCOMPLETE_MESSAGE = "DqlRecovery.Preview.PayloadIncomplete";
    private static final String PREVIEW_CURRENT_TASK_VERSION_UNAVAILABLE_MESSAGE =
            "DqlRecovery.Preview.CurrentTaskVersionUnavailable";
    private static final String PREVIEW_EVENT_TASK_VERSION_UNAVAILABLE_MESSAGE =
            "DqlRecovery.Preview.EventTaskVersionUnavailable";
    private static final String PREVIEW_TASK_VERSION_CHANGED_MESSAGE = "DqlRecovery.Preview.TaskVersionChanged";
    private static final String PREVIEW_AGENT_UNAVAILABLE_MESSAGE = "DqlRecovery.Preview.AgentUnavailable";
    private static final String STATUS_SYNC_TASK_NOT_FOUND_REASON = "Task.NotFound";
    private static final String[] TASK_FIELDS = {"name", "status", "version", "agentId"};

    private final DqlEventRepository eventRepository;
    private final DqlRecoveryBatchRepository batchRepository;
    private final DqlEventPermissionService permissionService;
    private final MessageQueueServiceImpl messageQueueService;
    private final DqlEventAlarmService alarmService;
    private final TaskService taskService;
    private final WorkerService workerService;
    private final DqlRecoveryTaskLockRepository taskLockRepository;
    private final DqlEventWebMapper webMapper;
    @Autowired(required = false)
    private SettingsService settingsService;

    public DqlRecoveryBatchService(DqlEventRepository eventRepository,
                                   DqlRecoveryBatchRepository batchRepository,
                                   DqlEventPermissionService permissionService,
                                   MessageQueueServiceImpl messageQueueService,
                                   DqlEventAlarmService alarmService) {
        this(eventRepository, batchRepository, permissionService, messageQueueService, alarmService,
                null, null, null);
    }

    public DqlRecoveryBatchService(DqlEventRepository eventRepository,
                                   DqlRecoveryBatchRepository batchRepository,
                                   DqlEventPermissionService permissionService,
                                   MessageQueueServiceImpl messageQueueService,
                                   DqlEventAlarmService alarmService,
                                   TaskService taskService,
                                   WorkerService workerService) {
        this(eventRepository, batchRepository, permissionService, messageQueueService, alarmService,
                taskService, workerService, null);
    }

    @Autowired
    public DqlRecoveryBatchService(DqlEventRepository eventRepository,
                                   DqlRecoveryBatchRepository batchRepository,
                                   DqlEventPermissionService permissionService,
                                   MessageQueueServiceImpl messageQueueService,
                                   DqlEventAlarmService alarmService,
                                   TaskService taskService,
                                   WorkerService workerService,
                                   DqlRecoveryTaskLockRepository taskLockRepository) {
        this.eventRepository = eventRepository;
        this.batchRepository = batchRepository;
        this.permissionService = permissionService;
        this.messageQueueService = messageQueueService;
        this.alarmService = alarmService;
        this.taskService = taskService;
        this.workerService = workerService;
        this.taskLockRepository = taskLockRepository;
        this.webMapper = new DqlEventWebMapper();
    }

    public DqlRecoveryPreviewVo preview(DqlRecoveryRequestVo request, UserDetail user) {
        return preview(request, user, true);
    }

    private DqlRecoveryPreviewVo preview(DqlRecoveryRequestVo request, UserDetail user, boolean checkActiveBatch) {
        List<String> eventIds = requireEventIds(request);
        checkMenuPermission(user);
        List<DqlEventDto> events = eventRepository.findByEventIds(eventIds);
        Set<String> taskIds = events.stream().map(DqlEventDto::getTaskId).collect(Collectors.toSet());
        if (permissionService != null) {
            taskIds.stream()
                    .sorted(Comparator.nullsFirst(String::compareTo))
                    .forEach(taskId -> permissionService.checkTaskVisible(taskId, user));
        }
        if (taskIds.size() > 1) {
            throw new BizException("DqlRecovery.CrossTaskNotAllowed", "eventIds");
        }
        DqlRecoveryPreviewVo preview = new DqlRecoveryPreviewVo();
        if (!events.isEmpty()) {
            DqlEventDto first = events.get(0);
            preview.setTaskId(first.getTaskId());
            preview.setTaskName(first.getTaskName());
        }

        RecoveryTaskContext taskContext = null;
        if (strictRecoveryValidation() && !events.isEmpty()) {
            taskContext = loadTaskContext(events.get(0).getTaskId());
            if (StringUtils.isNotBlank(taskContext.task().getName())) {
                preview.setTaskName(taskContext.task().getName());
            }
        }
        DqlRuntimeConfig config = runtimeConfig();
        boolean batchSizeExceeded = eventIds.size() > config.getRecoveryBatchMaxSize();
        DqlRecoveryBatchDto activeBatch = checkActiveBatch && !events.isEmpty()
                ? batchRepository.findActiveByTaskId(events.get(0).getTaskId()) : null;
        if (activeBatch != null && reconcileFinishedBatch(activeBatch)) {
            activeBatch = null;
        }
        boolean activeBatchExists = activeBatch != null;

        Map<String, DqlEventDto> eventMap = events.stream().collect(Collectors.toMap(DqlEventDto::getEventId, event -> event, (a, b) -> a, LinkedHashMap::new));
        Map<String, PreviewMessage> blockedReasons = new LinkedHashMap<>();
        Map<String, PreviewMessage> riskyReasons = new LinkedHashMap<>();
        for (String eventId : eventIds) {
            DqlEventDto event = eventMap.get(eventId);
            if (event == null) {
                blockedReasons.put(eventId, localizedPreviewMessage(PREVIEW_EVENT_NOT_FOUND_MESSAGE));
            } else {
                PreviewMessage reason = previewBlockedReason(
                        event, taskContext, batchSizeExceeded, activeBatchExists, config);
                if (reason != null) {
                    blockedReasons.put(eventId, reason);
                } else if (isBusinessKeyRisk(event)) {
                    riskyReasons.put(eventId, localizedPreviewMessage(PREVIEW_EVENT_NO_BUSINESS_KEY_MESSAGE));
                }
            }
        }

        for (String eventId : eventIds) {
            if (riskyReasons.containsKey(eventId)) {
                preview.getRiskyEvents().add(blocked(eventId, riskyReasons.get(eventId), eventMap.get(eventId)));
            }
            if (blockedReasons.containsKey(eventId)) {
                preview.getBlockedEvents().add(blocked(eventId, blockedReasons.get(eventId), eventMap.get(eventId)));
            }
        }

        DqlRecoveryOrder.sort(events.stream()
                        .filter(event -> !blockedReasons.containsKey(event.getEventId()))
                        .toList())
                .stream()
                .map(this::orderedEvent)
                .forEach(preview.getOrderedEvents()::add);
        preview.setCanSubmit(preview.getBlockedEvents().isEmpty() && preview.getOrderedEvents().size() == eventIds.size());
        if (!preview.isCanSubmit()) {
            preview.setMessage(localizedPreviewMessage(PREVIEW_SUMMARY_MESSAGE).message());
        }
        return preview;
    }

    /**
     * Synchronizes only durable recovery blockers into the event lifecycle status.
     * Runtime conditions such as Agent liveness, an active batch and request batch size
     * are deliberately excluded because they do not make the event permanently invalid.
     */
    public int synchronizeNotReprocessableEvents() {
        Map<String, StatusSyncTaskLookup> taskCache = new LinkedHashMap<>();
        int changed = synchronizePendingEvents(taskCache);
        changed += restoreEventsWhoseTaskVersionRecovered(taskCache);
        return changed;
    }

    private int synchronizePendingEvents(Map<String, StatusSyncTaskLookup> taskCache) {
        List<DqlEventDto> events = eventRepository.findReprocessableEventsForStatusSync();
        if (events == null || events.isEmpty()) {
            return 0;
        }
        int changed = 0;
        for (DqlEventDto event : events) {
            if (event == null || StringUtils.isBlank(event.getEventId())) {
                continue;
            }
            DqlEventStatusEnum status = DqlEventStatusEnum.parse(event.getStatus());
            String reasonCode = statusSyncBlockReason(event, taskCache);
            if (status == null || !status.reprocessable() || reasonCode == null) {
                continue;
            }
            if (eventRepository.markNotReprocessable(event.getEventId(), status, reasonCode, new Date())) {
                changed++;
            }
        }
        return changed;
    }

    private int restoreEventsWhoseTaskVersionRecovered(Map<String, StatusSyncTaskLookup> taskCache) {
        List<DqlEventDto> events = eventRepository.findSyncedNotReprocessableEvents();
        if (events == null || events.isEmpty() || !strictRecoveryValidation()) {
            return 0;
        }
        int changed = 0;
        for (DqlEventDto event : events) {
            if (event == null || StringUtils.isBlank(event.getEventId())
                    || !isTaskVersionReason(event.getNotReprocessableReason())) {
                continue;
            }
            DqlEventStatusEnum originalStatus = DqlEventStatusEnum.parse(event.getRecoveryStatusBeforeSync());
            if (originalStatus == null || !originalStatus.reprocessable()) {
                continue;
            }
            StatusSyncTaskLookup taskLookup = statusSyncTask(event.getTaskId(), taskCache);
            if (taskLookup.state() != StatusSyncTaskLookupState.FOUND
                    || taskVersionBlockReason(event, taskLookup.task()) != null) {
                continue;
            }
            if (eventRepository.restoreReprocessableStatus(event.getEventId(), originalStatus,
                    event.getNotReprocessableReason(), new Date())) {
                changed++;
            }
        }
        return changed;
    }

    private String statusSyncBlockReason(DqlEventDto event,
                                         Map<String, StatusSyncTaskLookup> taskCache) {
        if (Boolean.FALSE.equals(event.getPayloadComplete())) {
            return PREVIEW_PAYLOAD_INCOMPLETE_MESSAGE;
        }
        if (!strictRecoveryValidation()) {
            return null;
        }
        StatusSyncTaskLookup taskLookup = statusSyncTask(event.getTaskId(), taskCache);
        if (taskLookup.state() == StatusSyncTaskLookupState.UNAVAILABLE) {
            return null;
        }
        if (taskLookup.state() == StatusSyncTaskLookupState.NOT_FOUND) {
            return STATUS_SYNC_TASK_NOT_FOUND_REASON;
        }
        return taskVersionBlockReason(event, taskLookup.task());
    }

    private String taskVersionBlockReason(DqlEventDto event, TaskDto task) {
        if (task == null || task.getVersion() == null || task.getVersion() < 1L) {
            return PREVIEW_CURRENT_TASK_VERSION_UNAVAILABLE_MESSAGE;
        }
        if (event.getTaskVersion() == null || event.getTaskVersion() < 1L) {
            return PREVIEW_EVENT_TASK_VERSION_UNAVAILABLE_MESSAGE;
        }
        if (!task.getVersion().equals(event.getTaskVersion())) {
            return PREVIEW_TASK_VERSION_CHANGED_MESSAGE;
        }
        return null;
    }

    private boolean isTaskVersionReason(String reasonCode) {
        return StringUtils.equals(reasonCode, STATUS_SYNC_TASK_NOT_FOUND_REASON)
                || StringUtils.equals(reasonCode, PREVIEW_CURRENT_TASK_VERSION_UNAVAILABLE_MESSAGE)
                || StringUtils.equals(reasonCode, PREVIEW_EVENT_TASK_VERSION_UNAVAILABLE_MESSAGE)
                || StringUtils.equals(reasonCode, PREVIEW_TASK_VERSION_CHANGED_MESSAGE);
    }

    private StatusSyncTaskLookup statusSyncTask(String taskId,
                                                Map<String, StatusSyncTaskLookup> taskCache) {
        if (taskCache.containsKey(taskId)) {
            return taskCache.get(taskId);
        }
        StatusSyncTaskLookup lookup;
        if (StringUtils.isBlank(taskId) || !ObjectId.isValid(taskId)) {
            lookup = new StatusSyncTaskLookup(StatusSyncTaskLookupState.NOT_FOUND, null);
        } else if (!strictRecoveryValidation()) {
            lookup = new StatusSyncTaskLookup(StatusSyncTaskLookupState.UNAVAILABLE, null);
        } else {
            try {
                TaskDto task = taskService.findByTaskId(new ObjectId(taskId), TASK_FIELDS);
                lookup = task == null
                        ? new StatusSyncTaskLookup(StatusSyncTaskLookupState.NOT_FOUND, null)
                        : new StatusSyncTaskLookup(StatusSyncTaskLookupState.FOUND, task);
            } catch (RuntimeException exception) {
                log.warn("DQL recovery status synchronization could not read task {}: {}",
                        taskId, exception.getMessage());
                lookup = new StatusSyncTaskLookup(StatusSyncTaskLookupState.UNAVAILABLE, null);
            }
        }
        taskCache.put(taskId, lookup);
        return lookup;
    }

    public DqlRecoveryBatchDto start(DqlRecoveryRequestVo request, UserDetail user) {
        requireEventIds(request);
        if (!Boolean.TRUE.equals(request.getConfirm())) {
            throw new BizException("IllegalArgument", "confirm");
        }
        String mode = requireMode(request.getMode());
        DqlRecoveryPreviewVo preview = preview(request, user, false);
        if (!preview.isCanSubmit()) {
            throw new BizException("DqlRecovery.EventNotReprocessable", preview.getMessage());
        }
        List<String> orderedEventIds = preview.getOrderedEvents().stream()
                .map(DqlRecoveryPreviewVo.OrderedEvent::getEventId)
                .toList();
        List<DqlEventDto> events = eventRepository.findByEventIds(orderedEventIds);
        Map<String, DqlEventStatusEnum> originalEventStatuses = originalEventStatuses(events);
        RecoveryTaskContext taskContext = strictRecoveryValidation() ? loadTaskContext(preview.getTaskId()) : null;
        String batchId = buildBatchId();
        if (!acquireTaskLock(preview.getTaskId(), batchId)) {
            throw new BizException("DqlRecovery.BatchAlreadyRunning", preview.getTaskId());
        }
        DqlRecoveryBatchDto batch = new DqlRecoveryBatchDto();
        batch.setBatchId(batchId);
        batch.setTaskId(preview.getTaskId());
        batch.setTaskName(preview.getTaskName());
        batch.setTaskStatusBefore(taskContext == null ? null : taskContext.task().getStatus());
        batch.setTaskStatusAfter(batch.getTaskStatusBefore());
        batch.setTaskVersion(taskContext == null
                ? (events.isEmpty() ? null : events.get(0).getTaskVersion())
                : taskContext.task().getVersion());
        batch.setAgentId(taskContext == null
                ? (events.isEmpty() ? null : events.get(0).getAgentId())
                : taskContext.task().getAgentId());
        batch.setEventIds(requireEventIds(request));
        batch.setOrderedEventIds(orderedEventIds);
        batch.setOperatorId(user == null ? null : user.getUserId());
        batch.setOperatorName(user == null ? null : user.getUsername());
        batch.setMode(mode);
        batch.setStatus(DqlRecoveryBatchStatusEnum.CREATED.name());
        batch.setSelectedCount(orderedEventIds.size());
        batch.setSuccessCount(0);
        batch.setFailedCount(0);
        batch.setSkippedCount(0);
        batch.setAuditEntries(new ArrayList<>(List.of(auditEntry(
                AUDIT_BATCH_CREATED,
                DqlRecoveryBatchStatusEnum.CREATED.name(),
                null,
                null,
                null,
                batch.getOperatorId(),
                batch.getOperatorName()))));
        try {
            batch = batchRepository.create(batch);
            if (batch == null || StringUtils.isBlank(batch.getBatchId())) {
                throw new IllegalStateException("Recovery batch creation returned no batch");
            }
        } catch (RuntimeException e) {
            releaseTaskLock(preview.getTaskId(), batchId);
            throw e;
        }

        long locked;
        try {
            locked = eventRepository.lockEvents(orderedEventIds, batch.getBatchId());
        } catch (RuntimeException e) {
            try {
                eventRepository.releaseBatchLocks(batch.getBatchId(), originalEventStatuses);
                batchRepository.finish(batch.getBatchId(), DqlRecoveryBatchStatusEnum.CANCELED, e.getMessage());
                appendAudit(batch, DqlRecoveryBatchStatusEnum.CANCELED.name(), AUDIT_BATCH_FAILED, null, null, e.getMessage());
            } finally {
                releaseTaskLock(preview.getTaskId(), batchId);
            }
            throw e;
        }
        if (locked != orderedEventIds.size()) {
            try {
                eventRepository.releaseBatchLocks(batch.getBatchId(), originalEventStatuses);
                batchRepository.finish(batch.getBatchId(), DqlRecoveryBatchStatusEnum.CANCELED, "Failed to lock selected events");
                appendAudit(batch, DqlRecoveryBatchStatusEnum.CANCELED.name(), AUDIT_BATCH_FAILED,
                        null, null, "Failed to lock selected events");
            } finally {
                releaseTaskLock(preview.getTaskId(), batchId);
            }
            throw new BizException("DqlRecovery.EventLockFailed", batch.getBatchId());
        }
        try {
            batchRepository.updateStatus(batch.getBatchId(), DqlRecoveryBatchStatusEnum.DISPATCHED, null);
            batch.setStatus(DqlRecoveryBatchStatusEnum.DISPATCHED.name());
            dispatch(batch);
            appendAudit(batch, DqlRecoveryBatchStatusEnum.DISPATCHED.name(), AUDIT_BATCH_DISPATCHED,
                    null, null, null);
            return batch;
        } catch (RuntimeException e) {
            try {
                eventRepository.releaseBatchLocks(batch.getBatchId(), originalEventStatuses);
                batchRepository.finish(batch.getBatchId(), DqlRecoveryBatchStatusEnum.FAILED, e.getMessage());
                appendAudit(batch, DqlRecoveryBatchStatusEnum.FAILED.name(), AUDIT_BATCH_FAILED,
                        null, null, e.getMessage());
            } finally {
                releaseTaskLock(preview.getTaskId(), batchId);
            }
            throw e;
        }
    }

    public void report(String taskId, DqlRecoveryResultReportVo report) {
        if (report == null || StringUtils.isBlank(report.getBatchId())) {
            throw new BizException("IllegalArgument", "batchId");
        }
        DqlRecoveryBatchDto batch = batchRepository.findByBatchId(report.getBatchId());
        if (batch == null) {
            throw new BizException("DqlRecovery.BatchNotFound", report.getBatchId());
        }
        if (!StringUtils.equals(taskId, batch.getTaskId())) {
            throw new BizException("IllegalArgument", "taskId");
        }
        DqlRecoveryReportTypeEnum type = Optional.ofNullable(DqlRecoveryReportTypeEnum.parse(report.getType()))
                .orElseThrow(() -> new BizException("IllegalArgument", "type"));
        switch (type) {
            case BATCH_STARTED -> handleBatchStarted(batch);
            case BATCH_HEARTBEAT -> handleBatchHeartbeat(batch);
            case EVENT_STARTED -> handleEventStarted(batch, report);
            case EVENT_RESULT -> handleEventResult(batch, report);
            case BATCH_FINISHED -> finishBatch(batch, report);
            case BATCH_FAILED -> failBatch(batch, report.getMessage(), report.getNodeStates());
            default -> throw new BizException("IllegalArgument", "type");
        }
    }

    /**
     * Compensates active batches that have not received progress within the batch timeout.
     * The repository methods use conditional updates so a concurrent callback wins for any
     * event it completed before the timeout scan.
     */
    public int timeoutExpiredBatches(Date now) {
        Date current = now == null ? new Date() : now;
        DqlRuntimeConfig config = runtimeConfig();
        Date dispatchDeadline = deadline(current, config.getRecoveryDispatchTimeoutSeconds());
        Date heartbeatDeadline = deadline(current, config.getRecoveryHeartbeatTimeoutSeconds());
        Date legacyDeadline = deadline(current, config.getRecoveryBatchTimeoutSeconds());
        List<DqlRecoveryBatchDto> timedOut = Optional.ofNullable(batchRepository.findTimedOut(
                        dispatchDeadline, heartbeatDeadline, legacyDeadline))
                .orElse(List.of());
        int finalized = 0;
        for (DqlRecoveryBatchDto batch : timedOut) {
            if (batch == null || StringUtils.isBlank(batch.getBatchId())) {
                continue;
            }
            DqlRecoveryBatchDto latest = Optional.ofNullable(batchRepository.findByBatchId(batch.getBatchId()))
                    .orElse(batch);
            if (!isTimedOut(latest, current, config)) {
                continue;
            }
            long timedOutEvents = eventRepository.timeoutEvents(
                    batch.getBatchId(), batchEventIds(batch), current);
            if (timedOutEvents > 0) {
                batchRepository.increaseFailed(batch.getBatchId(), Math.toIntExact(timedOutEvents));
            }
            if (eventRepository.countReprocessingByBatchId(batch.getBatchId()) > 0) {
                continue;
            }
            DqlRecoveryBatchStatusEnum timeoutStatus = timeoutStatus(latest);
            String timeoutMessage = timeoutMessage(latest);
            if (batchRepository.finishTimedOut(
                    latest.getBatchId(), timeoutStatus, timeoutMessage,
                    dispatchDeadline, heartbeatDeadline, legacyDeadline)) {
                appendAudit(latest, timeoutStatus.name(), AUDIT_BATCH_TIMEOUT,
                        null, null, timeoutMessage);
                alarmService.notifyRecoveryFailed(latest);
                releaseTaskLock(latest);
                finalized++;
            }
        }
        return finalized;
    }

    public DqlRecoveryBatchDto detail(String batchId, UserDetail user) {
        checkMenuPermission(user);
        DqlRecoveryBatchDto batch = batchRepository.findByBatchId(batchId);
        if (batch == null) {
            throw new BizException("DqlRecovery.BatchNotFound", batchId);
        }
        if (permissionService != null) {
            permissionService.checkTaskVisible(batch.getTaskId(), user);
        }
        normalizeDetail(batch);
        return batch;
    }

    /**
     * Records a source-read gate result for a batch. The result is written to
     * the detail fields and its audit entry in one repository update so the
     * detail API never exposes a gate result without the corresponding trace.
     */
    public void recordSourceReadResult(String batchId,
                                       boolean pause,
                                       String result,
                                       String message,
                                       Long occurredAt) {
        if (StringUtils.isBlank(batchId)) {
            throw new BizException("IllegalArgument", "batchId");
        }
        if (StringUtils.isBlank(result)) {
            throw new BizException("IllegalArgument", "result");
        }
        Date at = occurredAt == null ? new Date() : new Date(occurredAt);
        DqlRecoveryAuditEntryDto entry = auditEntry(
                pause ? AUDIT_SOURCE_READ_PAUSE : AUDIT_SOURCE_READ_RESUME,
                result,
                null,
                null,
                message,
                null,
                null);
        entry.setOccurredAt(at);
        batchRepository.recordSourceReadResult(batchId, pause, result, message, at, entry);
    }

    private void checkMenuPermission(UserDetail user) {
        if (permissionService != null) {
            permissionService.checkMenuVisible(user);
        }
    }

    private void handleEventResult(DqlRecoveryBatchDto batch, DqlRecoveryResultReportVo report) {
        requireEventReport(batch, report);
        DqlRecoveryAttemptResultEnum result = Optional.ofNullable(DqlRecoveryAttemptResultEnum.parse(report.getResult()))
                .orElseThrow(() -> new BizException("IllegalArgument", "result"));
        if (result == DqlRecoveryAttemptResultEnum.RUNNING) {
            throw new BizException("IllegalArgument", "result");
        }
        if (requireEventCallbackBatchState(batch, report, result, false)) {
            return;
        }
        DqlRecoveryAttemptDto attempt = attempt(batch, report, result);
        if (result == DqlRecoveryAttemptResultEnum.SUCCESS) {
            DqlRecoveryCallbackResultEnum transition = eventRepository.completeEventIdempotent(
                    report.getEventId(), batch.getBatchId(), attempt);
            if (transition == DqlRecoveryCallbackResultEnum.APPLIED) {
                batchRepository.increaseSuccess(batch.getBatchId());
                appendAudit(batch, result.name(), AUDIT_EVENT_RESULT,
                        report.getEventId(), report.getAttemptId(), report.getMessage());
            } else if (transition == DqlRecoveryCallbackResultEnum.CONFLICT) {
                throw new BizException("DqlRecovery.AttemptConflict", report.getAttemptId());
            } else if (transition == DqlRecoveryCallbackResultEnum.NOT_IN_BATCH) {
                throw new BizException("DqlRecovery.EventNotInBatch", report.getEventId());
            }
        } else if (result == DqlRecoveryAttemptResultEnum.SKIPPED) {
            DqlRecoveryCallbackResultEnum transition = eventRepository.failEventIdempotent(
                    report.getEventId(), batch.getBatchId(), attempt);
            if (transition == DqlRecoveryCallbackResultEnum.APPLIED) {
                batchRepository.increaseSkipped(batch.getBatchId());
                appendAudit(batch, result.name(), AUDIT_EVENT_RESULT,
                        report.getEventId(), report.getAttemptId(), report.getMessage());
            } else if (transition == DqlRecoveryCallbackResultEnum.CONFLICT) {
                throw new BizException("DqlRecovery.AttemptConflict", report.getAttemptId());
            } else if (transition == DqlRecoveryCallbackResultEnum.NOT_IN_BATCH) {
                throw new BizException("DqlRecovery.EventNotInBatch", report.getEventId());
            }
        } else {
            DqlRecoveryCallbackResultEnum transition = eventRepository.failEventIdempotent(
                    report.getEventId(), batch.getBatchId(), attempt);
            if (transition == DqlRecoveryCallbackResultEnum.APPLIED) {
                batchRepository.increaseFailed(batch.getBatchId());
                appendAudit(batch, result.name(), AUDIT_EVENT_RESULT,
                        report.getEventId(), report.getAttemptId(), report.getMessage());
                alarmService.notifyRecoveryFailed(batch);
            } else if (transition == DqlRecoveryCallbackResultEnum.CONFLICT) {
                throw new BizException("DqlRecovery.AttemptConflict", report.getAttemptId());
            } else if (transition == DqlRecoveryCallbackResultEnum.NOT_IN_BATCH) {
                throw new BizException("DqlRecovery.EventNotInBatch", report.getEventId());
            }
        }
        tryFinalizeRequestedBatch(batch.getBatchId());
    }

    private void finishBatch(DqlRecoveryBatchDto batch, DqlRecoveryResultReportVo report) {
        DqlRecoveryBatchStatusEnum actual = DqlRecoveryBatchStatusEnum.parse(batch.getStatus());
        if (isTerminal(actual)) {
            releaseTaskLock(batch);
            return;
        }
        // BATCH_STARTED and BATCH_FINISHED are reported asynchronously. A
        // finish report may therefore arrive while the batch is still CREATED
        // or DISPATCHED. The finish report is authoritative: accepting it here
        // prevents a late BATCH_STARTED callback from leaving the batch and
        // task lock stuck in RUNNING forever.
        requireBatchStatus(batch, DqlRecoveryBatchStatusEnum.CREATED,
                DqlRecoveryBatchStatusEnum.DISPATCHED,
                DqlRecoveryBatchStatusEnum.RUNNING);
        boolean terminalized = false;
        try {
            if (report.getNodeStates() != null) {
                batchRepository.updateNodeStates(batch.getBatchId(), report.getNodeStates());
            }
            int selected = Optional.ofNullable(batch.getSelectedCount()).orElse(0);
            int success = Optional.ofNullable(batch.getSuccessCount()).orElse(0);
            int failed = Optional.ofNullable(batch.getFailedCount()).orElse(0);
            int skipped = Optional.ofNullable(batch.getSkippedCount()).orElse(0);
            if (selected < 0 || success < 0 || failed < 0 || skipped < 0
                    || selected != success + failed + skipped) {
                batchRepository.recordFinishRequested(batch.getBatchId(), report.getMessage());
                tryFinalizeRequestedBatch(batch.getBatchId());
                return;
            }
            DqlRecoveryBatchStatusEnum status = failed == 0 && skipped == 0
                    ? DqlRecoveryBatchStatusEnum.SUCCESS
                    : DqlRecoveryBatchStatusEnum.PARTIAL_FAILED;
            batchRepository.finish(batch.getBatchId(), status, report.getMessage());
            batch.setStatus(status.name());
            batch.setFinishedAt(new Date());
            batch.setFinishRequested(false);
            terminalized = true;
            appendAudit(batch, status.name(), AUDIT_BATCH_FINISHED,
                    null, null, report.getMessage());
            if (selected > 0 && status == DqlRecoveryBatchStatusEnum.PARTIAL_FAILED) {
                alarmService.notifyBatchPartialFailed(batch);
            }
        } finally {
            if (terminalized) {
                releaseTaskLock(batch);
            }
        }
    }

    /**
     * Completes a finish callback that was observed before the final event
     * counter update. The repository transition is conditional on the
     * pending marker, so concurrent event callbacks cannot finalize twice.
     */
    private void tryFinalizeRequestedBatch(String batchId) {
        DqlRecoveryBatchDto latest = batchRepository.findByBatchId(batchId);
        if (latest == null || !Boolean.TRUE.equals(latest.getFinishRequested())) {
            return;
        }
        DqlRecoveryBatchStatusEnum actual = DqlRecoveryBatchStatusEnum.parse(latest.getStatus());
        if (actual != DqlRecoveryBatchStatusEnum.CREATED
                && actual != DqlRecoveryBatchStatusEnum.DISPATCHED
                && actual != DqlRecoveryBatchStatusEnum.RUNNING) {
            return;
        }
        int selected = Optional.ofNullable(latest.getSelectedCount()).orElse(0);
        int success = Optional.ofNullable(latest.getSuccessCount()).orElse(0);
        int failed = Optional.ofNullable(latest.getFailedCount()).orElse(0);
        int skipped = Optional.ofNullable(latest.getSkippedCount()).orElse(0);
        if (selected < 0 || success < 0 || failed < 0 || skipped < 0
                || selected != success + failed + skipped) {
            return;
        }
        DqlRecoveryBatchStatusEnum status = failed == 0 && skipped == 0
                ? DqlRecoveryBatchStatusEnum.SUCCESS
                : DqlRecoveryBatchStatusEnum.PARTIAL_FAILED;
        if (!batchRepository.finishRequested(batchId, status, latest.getFinishMessage())) {
            return;
        }
        latest.setStatus(status.name());
        latest.setFinishedAt(new Date());
        latest.setFinishRequested(false);
        try {
            appendAudit(latest, status.name(), AUDIT_BATCH_FINISHED,
                    null, null, latest.getFinishMessage());
            if (selected > 0 && status == DqlRecoveryBatchStatusEnum.PARTIAL_FAILED) {
                alarmService.notifyBatchPartialFailed(latest);
            }
        } finally {
            releaseTaskLock(latest);
        }
    }

    private void failBatch(DqlRecoveryBatchDto batch, String message,
                           List<DqlRecoveryNodeStateDto> nodeStates) {
        if (isTerminal(DqlRecoveryBatchStatusEnum.parse(batch.getStatus()))) {
            releaseTaskLock(batch);
            return;
        }
        requireBatchStatus(batch, DqlRecoveryBatchStatusEnum.CREATED,
                DqlRecoveryBatchStatusEnum.DISPATCHED, DqlRecoveryBatchStatusEnum.RUNNING);
        try {
            if (nodeStates != null) {
                batchRepository.updateNodeStates(batch.getBatchId(), nodeStates);
            }
            Date now = new Date();
            eventRepository.finalizeRunningAttempts(batch.getBatchId(), batchEventIds(batch),
                    DqlRecoveryAttemptResultEnum.FAILED, message, now);
            eventRepository.finalizeUnstartedAttempts(batch.getBatchId(), batchEventIds(batch),
                    batchFailureAttempt(batch, message, now), now);
            eventRepository.releaseBatchLocks(batch.getBatchId(), DqlEventStatusEnum.RECOVERY_FAILED);
            batchRepository.finish(batch.getBatchId(), DqlRecoveryBatchStatusEnum.FAILED, message);
            appendAudit(batch, DqlRecoveryBatchStatusEnum.FAILED.name(), AUDIT_BATCH_FAILED,
                    null, null, message);
            alarmService.notifyRecoveryFailed(batch);
        } finally {
            releaseTaskLock(batch);
        }
    }

    private DqlRecoveryAttemptDto batchFailureAttempt(DqlRecoveryBatchDto batch,
                                                      String message,
                                                      Date now) {
        DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
        attempt.setAttemptId("BATCH_FAILED-" + batch.getBatchId());
        attempt.setBatchId(batch.getBatchId());
        attempt.setOperatorId(batch.getOperatorId());
        attempt.setOperatorName(batch.getOperatorName());
        attempt.setTaskVersion(batch.getTaskVersion());
        Date startedAt = Optional.ofNullable(batch.getStartedAt())
                .orElse(Optional.ofNullable(batch.getUpdated()).orElse(now));
        attempt.setStartedAt(startedAt);
        attempt.setFinishedAt(now);
        attempt.setResult(DqlRecoveryAttemptResultEnum.FAILED.name());
        attempt.setMessage(message);
        return attempt;
    }

    private void handleBatchStarted(DqlRecoveryBatchDto batch) {
        DqlRecoveryBatchStatusEnum actual = DqlRecoveryBatchStatusEnum.parse(batch.getStatus());
        if (actual == DqlRecoveryBatchStatusEnum.RUNNING || isTerminal(actual)) {
            return;
        }
        requireBatchStatus(batch, DqlRecoveryBatchStatusEnum.CREATED,
                DqlRecoveryBatchStatusEnum.DISPATCHED);
        if (batchRepository.markRunning(batch.getBatchId())) {
            appendAudit(batch, DqlRecoveryBatchStatusEnum.RUNNING.name(), AUDIT_BATCH_STARTED,
                    null, null, null);
        }
    }

    private void handleBatchHeartbeat(DqlRecoveryBatchDto batch) {
        DqlRecoveryBatchStatusEnum actual = DqlRecoveryBatchStatusEnum.parse(batch.getStatus());
        if (isTerminal(actual)) {
            return;
        }
        requireBatchStatus(batch, DqlRecoveryBatchStatusEnum.RUNNING);
        // TM receipt time is authoritative for liveness. Engine clocks must not
        // be able to extend the batch lease with a future timestamp.
        batchRepository.touchHeartbeat(batch.getBatchId(), new Date());
    }

    private void handleEventStarted(DqlRecoveryBatchDto batch, DqlRecoveryResultReportVo report) {
        requireEventReport(batch, report);
        if (requireEventCallbackBatchState(batch, report, DqlRecoveryAttemptResultEnum.RUNNING, true)) {
            return;
        }
        DqlRecoveryAttemptDto attempt = attempt(batch, report, DqlRecoveryAttemptResultEnum.RUNNING);
        DqlRecoveryCallbackResultEnum transition = eventRepository.startEventIdempotent(
                report.getEventId(), batch.getBatchId(), attempt);
        if (transition == DqlRecoveryCallbackResultEnum.NOT_IN_BATCH) {
            throw new BizException("DqlRecovery.EventNotInBatch", report.getEventId());
        }
        if (transition == DqlRecoveryCallbackResultEnum.CONFLICT) {
            throw new BizException("DqlRecovery.AttemptConflict", report.getAttemptId());
        }
        if (transition == DqlRecoveryCallbackResultEnum.APPLIED) {
            appendAudit(batch, DqlRecoveryAttemptResultEnum.RUNNING.name(), AUDIT_EVENT_STARTED,
                    report.getEventId(), report.getAttemptId(), report.getMessage());
        }
    }

    private boolean requireEventCallbackBatchState(DqlRecoveryBatchDto batch,
                                                   DqlRecoveryResultReportVo report,
                                                   DqlRecoveryAttemptResultEnum result,
                                                   boolean started) {
        DqlRecoveryBatchStatusEnum actual = DqlRecoveryBatchStatusEnum.parse(batch.getStatus());
        if (actual == DqlRecoveryBatchStatusEnum.RUNNING) {
            return false;
        }
        if (isTerminal(actual) && duplicateEventCallback(batch, report, result, started)) {
            return true;
        }
        throw new BizException("DqlRecovery.InvalidBatchState", batch.getBatchId());
    }

    private boolean duplicateEventCallback(DqlRecoveryBatchDto batch,
                                            DqlRecoveryResultReportVo report,
                                            DqlRecoveryAttemptResultEnum result,
                                            boolean started) {
        DqlEventDto event = eventRepository.findByEventId(report.getEventId());
        if (event == null || event.getRecoveryAttempts() == null) {
            return false;
        }
        return event.getRecoveryAttempts().stream()
                .filter(attempt -> StringUtils.equals(batch.getBatchId(), attempt.getBatchId()))
                .filter(attempt -> StringUtils.equals(report.getAttemptId(), attempt.getAttemptId()))
                .anyMatch(attempt -> started || StringUtils.equals(result.name(), attempt.getResult()));
    }

    private boolean isTerminal(DqlRecoveryBatchStatusEnum status) {
        return status == DqlRecoveryBatchStatusEnum.SUCCESS
                || status == DqlRecoveryBatchStatusEnum.PARTIAL_FAILED
                || status == DqlRecoveryBatchStatusEnum.FAILED
                || status == DqlRecoveryBatchStatusEnum.CANCELED;
    }

    private void requireBatchStatus(DqlRecoveryBatchDto batch, DqlRecoveryBatchStatusEnum... expected) {
        DqlRecoveryBatchStatusEnum actual = DqlRecoveryBatchStatusEnum.parse(batch.getStatus());
        for (DqlRecoveryBatchStatusEnum status : expected) {
            if (status == actual) {
                return;
            }
        }
        throw new BizException("DqlRecovery.InvalidBatchState", batch.getBatchId());
    }

    private void requireEventReport(DqlRecoveryBatchDto batch, DqlRecoveryResultReportVo report) {
        if (StringUtils.isBlank(report.getEventId()) || !batchEventIds(batch).contains(report.getEventId())) {
            throw new BizException("IllegalArgument", "eventId");
        }
        if (StringUtils.isBlank(report.getAttemptId())) {
            throw new BizException("IllegalArgument", "attemptId");
        }
    }

    private List<String> batchEventIds(DqlRecoveryBatchDto batch) {
        if (batch.getOrderedEventIds() != null && !batch.getOrderedEventIds().isEmpty()) {
            return batch.getOrderedEventIds();
        }
        return Optional.ofNullable(batch.getEventIds()).orElse(List.of());
    }

    private DqlRecoveryAttemptDto attempt(DqlRecoveryBatchDto batch,
                                          DqlRecoveryResultReportVo report,
                                          DqlRecoveryAttemptResultEnum result) {
        DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
        attempt.setAttemptId(report.getAttemptId());
        attempt.setBatchId(batch.getBatchId());
        attempt.setOperatorId(batch.getOperatorId());
        attempt.setOperatorName(batch.getOperatorName());
        attempt.setTaskVersion(batch.getTaskVersion());
        attempt.setStartedAt(report.getStartedAt() == null ? new Date() : new Date(report.getStartedAt()));
        attempt.setFinishedAt(result == DqlRecoveryAttemptResultEnum.RUNNING
                ? null
                : (report.getFinishedAt() == null ? new Date() : new Date(report.getFinishedAt())));
        attempt.setResult(result.name());
        attempt.setMessage(report.getMessage());
        attempt.setErrorCode(report.getErrorCode());
        attempt.setErrorDetails(report.getErrorDetails());
        return attempt;
    }

    private void dispatch(DqlRecoveryBatchDto batch) {
        if (messageQueueService == null || StringUtils.isBlank(batch.getAgentId())) {
            return;
        }
        DqlRecoveryMessageDto message = DqlRecoveryMessageDto.fromBatch(batch);
        messageQueueService.sendPipeMessage(message.toPayload(), "tm", batch.getAgentId());
    }

    private boolean isReprocessable(DqlEventDto event) {
        DqlEventStatusEnum status = DqlEventStatusEnum.parse(event.getStatus());
        return status != null && status.reprocessable() && !Boolean.FALSE.equals(event.getPayloadComplete());
    }

    private DqlRecoveryBatchStatusEnum timeoutStatus(DqlRecoveryBatchDto batch) {
        int success = Optional.ofNullable(batch.getSuccessCount()).orElse(0);
        return success > 0
                ? DqlRecoveryBatchStatusEnum.PARTIAL_FAILED
                : DqlRecoveryBatchStatusEnum.FAILED;
    }

    private String timeoutMessage(DqlRecoveryBatchDto batch) {
        DqlRecoveryBatchStatusEnum status = DqlRecoveryBatchStatusEnum.parse(batch.getStatus());
        if (status == DqlRecoveryBatchStatusEnum.DISPATCHED) {
            return RECOVERY_DISPATCH_TIMEOUT_MESSAGE;
        }
        if (status == DqlRecoveryBatchStatusEnum.RUNNING && batch.getPingTime() != null) {
            return RECOVERY_HEARTBEAT_TIMEOUT_MESSAGE;
        }
        return RECOVERY_BATCH_TIMEOUT_MESSAGE;
    }

    private Date deadline(Date now, long timeoutSeconds) {
        return new Date(now.getTime() - timeoutSeconds * 1000L);
    }

    private boolean isTimedOut(DqlRecoveryBatchDto batch,
                               Date now,
                               DqlRuntimeConfig config) {
        if (batch == null || now == null || config == null) {
            return false;
        }
        DqlRecoveryBatchStatusEnum status = DqlRecoveryBatchStatusEnum.parse(batch.getStatus());
        if (status == DqlRecoveryBatchStatusEnum.DISPATCHED) {
            return beforeOrAt(batch.getUpdated(), deadline(now, config.getRecoveryDispatchTimeoutSeconds()));
        }
        if (status != DqlRecoveryBatchStatusEnum.RUNNING) {
            return false;
        }
        if (batch.getPingTime() != null) {
            return beforeOrAt(batch.getPingTime(), deadline(now, config.getRecoveryHeartbeatTimeoutSeconds()));
        }
        return beforeOrAt(batch.getUpdated(), deadline(now, config.getRecoveryBatchTimeoutSeconds()));
    }

    private boolean beforeOrAt(Date value, Date deadline) {
        return value != null && !value.after(deadline);
    }

    private PreviewMessage previewBlockedReason(DqlEventDto event,
                                                RecoveryTaskContext taskContext,
                                                boolean batchSizeExceeded,
                                                boolean activeBatchExists,
                                                DqlRuntimeConfig config) {
        if (!isReprocessable(event)) {
            return blockedReason(event);
        }
        if (strictRecoveryValidation()) {
            PreviewMessage taskReason = taskContext.taskRecoveryReason(event);
            if (taskReason != null) {
                return taskReason;
            }
        }
        if (batchSizeExceeded) {
            return localizedPreviewMessage(PREVIEW_BATCH_SIZE_EXCEEDED_MESSAGE, config.getRecoveryBatchMaxSize());
        }
        if (activeBatchExists) {
            return localizedPreviewMessage(PREVIEW_ACTIVE_BATCH_EXISTS_MESSAGE);
        }
        return null;
    }

    private boolean isBusinessKeyRisk(DqlEventDto event) {
        return strictRecoveryValidation()
                && (Boolean.TRUE.equals(event.getEventKeyMissing())
                || StringUtils.isBlank(event.getRecordIdentity()));
    }

    private PreviewMessage blockedReason(DqlEventDto event) {
        DqlEventStatusEnum status = DqlEventStatusEnum.parse(event.getStatus());
        if (status == null || !status.reprocessable()) {
            return localizedPreviewMessage(PREVIEW_STATUS_NOT_REPROCESSABLE_MESSAGE, event.getStatus());
        }
        if (Boolean.FALSE.equals(event.getPayloadComplete())) {
            return localizedPreviewMessage(PREVIEW_PAYLOAD_INCOMPLETE_MESSAGE);
        }
        return localizedPreviewMessage(PREVIEW_EVENT_NOT_REPROCESSABLE_MESSAGE);
    }

    private boolean strictRecoveryValidation() {
        return taskService != null;
    }

    private String requireMode(String mode) {
        if (StringUtils.isBlank(mode)) {
            return DqlRecoveryMessageDto.MODE_AUTO;
        }
        if (!StringUtils.equals(DqlRecoveryMessageDto.MODE_AUTO, mode)) {
            throw new BizException("IllegalArgument", "mode");
        }
        return mode;
    }

    private void normalizeDetail(DqlRecoveryBatchDto batch) {
        if (StringUtils.isBlank(batch.getMode())) {
            batch.setMode(DqlRecoveryMessageDto.MODE_AUTO);
        }
        if (batch.getTaskStatusAfter() == null) {
            batch.setTaskStatusAfter(batch.getTaskStatusBefore());
        }
        if (batch.getAuditEntries() == null) {
            batch.setAuditEntries(new ArrayList<>());
        }
    }

    private void appendAudit(DqlRecoveryBatchDto batch,
                             String status,
                             String type,
                             String eventId,
                             String attemptId,
                             String message) {
        if (batch == null || StringUtils.isBlank(batch.getBatchId())) {
            return;
        }
        batchRepository.appendAudit(batch.getBatchId(), auditEntry(
                type,
                status,
                eventId,
                attemptId,
                message,
                batch.getOperatorId(),
                batch.getOperatorName()));
    }

    private DqlRecoveryAuditEntryDto auditEntry(String type,
                                                String status,
                                                String eventId,
                                                String attemptId,
                                                String message,
                                                String operatorId,
                                                String operatorName) {
        DqlRecoveryAuditEntryDto entry = new DqlRecoveryAuditEntryDto();
        entry.setType(type);
        entry.setStatus(status);
        entry.setEventId(eventId);
        entry.setAttemptId(attemptId);
        entry.setMessage(message);
        entry.setOccurredAt(new Date());
        entry.setOperatorId(operatorId);
        entry.setOperatorName(operatorName);
        return entry;
    }

    private RecoveryTaskContext loadTaskContext(String taskId) {
        if (StringUtils.isBlank(taskId) || !ObjectId.isValid(taskId)) {
            throw new BizException("Task.NotFound", taskId);
        }
        TaskDto task = taskService.findByTaskId(new ObjectId(taskId), TASK_FIELDS);
        if (task == null) {
            throw new BizException("Task.NotFound", taskId);
        }
        return new RecoveryTaskContext(task, agentAvailable(task.getAgentId()));
    }

    private boolean agentAvailable(String agentId) {
        if (workerService == null || StringUtils.isBlank(agentId)) {
            return false;
        }
        WorkerDto worker = workerService.queryWorkerByProcessId(agentId);
        return worker != null
                && !Boolean.TRUE.equals(worker.getStopping())
                && !Boolean.TRUE.equals(worker.getIsDeleted())
                && !Boolean.TRUE.equals(worker.getDeleted())
                && worker.getPingTime() != null
                && !workerService.isAgentTimeout(worker.getPingTime());
    }

    private DqlRecoveryPreviewVo.BlockedEvent blocked(String eventId, PreviewMessage previewMessage, DqlEventDto event) {
        DqlRecoveryPreviewVo.BlockedEvent blocked = new DqlRecoveryPreviewVo.BlockedEvent();
        blocked.setEventId(eventId);
        blocked.setMessageCode(previewMessage.code());
        blocked.setMessage(previewMessage.message());
        if (event != null) {
            blocked.setSourceTable(event.getSourceTable());
            blocked.setTargetTable(event.getTargetTable());
            blocked.setDmlType(event.getDmlType());
            blocked.setEventTime(event.getEventTime());
            blocked.setCaptureSeq(event.getCaptureSeq());
        }
        return blocked;
    }

    private static PreviewMessage localizedPreviewMessage(String code, Object... args) {
        return new PreviewMessage(code, MessageUtil.getMessage(code, args));
    }

    private DqlRecoveryPreviewVo.OrderedEvent orderedEvent(DqlEventDto event) {
        DqlEventDetailVo detail = webMapper.toDetail(event);
        DqlRecoveryPreviewVo.OrderedEvent ordered = new DqlRecoveryPreviewVo.OrderedEvent();
        ordered.setId(detail.getId());
        ordered.setEventId(detail.getEventId());
        ordered.setTaskId(detail.getTaskId());
        ordered.setTaskName(detail.getTaskName());
        ordered.setSourceTable(detail.getSourceTable());
        ordered.setTargetTable(detail.getTargetTable());
        ordered.setDmlType(detail.getDmlType());
        ordered.setErrorType(detail.getErrorType());
        ordered.setErrorCode(detail.getErrorCode());
        ordered.setEventTime(detail.getEventTime());
        ordered.setFailedAt(detail.getFailedAt());
        ordered.setCaptureSeq(detail.getCaptureSeq());
        ordered.setStatus(event.getStatus());
        ordered.setRecoveryCount(detail.getRecoveryCount());
        ordered.setLastRecoveryTime(detail.getLastRecoveryTime());
        ordered.setSourceNodeName(detail.getSourceNodeName());
        ordered.setTargetNodeName(detail.getTargetNodeName());
        ordered.setFailedNodeName(detail.getFailedNodeName());
        ordered.setStage(detail.getStage());
        ordered.setTableId(detail.getTableId());
        ordered.setEventKey(detail.getEventKey());
        ordered.setEventKeyMissing(detail.getEventKeyMissing());
        ordered.setPayloadFormat(detail.getPayloadFormat());
        ordered.setPayloadHash(detail.getPayloadHash());
        ordered.setPayloadSize(detail.getPayloadSize());
        ordered.setPayloadComplete(detail.getPayloadComplete());
        ordered.setPayloadPreview(detail.getPayloadPreview());
        ordered.setPayloadPreviewTruncated(detail.getPayloadPreviewTruncated());
        ordered.setErrorDetails(detail.getErrorDetails());
        ordered.setRawErrorRef(detail.getRawErrorRef());
        ordered.setOverwriteRisk(event.getOverwriteRisk());
        ordered.setOverwriteRiskMessage(event.getOverwriteRiskMessage());
        ordered.setLaterSuccessAt(event.getLaterSuccessAt());
        ordered.setLaterSuccessEventTime(event.getLaterSuccessEventTime());
        ordered.setLaterSuccessCaptureSeq(event.getLaterSuccessCaptureSeq());
        ordered.setLaterSuccessDmlType(event.getLaterSuccessDmlType());
        return ordered;
    }

    private List<String> requireEventIds(DqlRecoveryRequestVo request) {
        if (request == null || request.getEventIds() == null || request.getEventIds().isEmpty()) {
            throw new BizException("IllegalArgument", "eventIds");
        }
        return new ArrayList<>(request.getEventIds());
    }

    private String buildBatchId() {
        return "DQLB-" + new SimpleDateFormat("yyyyMMdd-HHmmssSSS").format(new Date());
    }

    private boolean acquireTaskLock(String taskId, String batchId) {
        if (taskLockRepository == null) {
            return true;
        }
        long leaseSeconds = runtimeConfig().getRecoveryBatchTimeoutSeconds();
        if (taskLockRepository.tryAcquire(taskId, batchId, leaseSeconds)) {
            return true;
        }

        // A terminal callback may persist the batch state before a transient
        // failure prevents deletion of the task lease. Reclaim a lock whose
        // batch is terminal, or whose active batch has fully reconciled event
        // counters; an active batch with outstanding events must still block
        // concurrent recovery submissions.
        DqlRecoveryTaskLockEntity currentLock;
        try {
            currentLock = taskLockRepository.findByTaskId(taskId);
        } catch (RuntimeException exception) {
            log.warn("DLQ recovery task lock owner lookup failed, taskId={}", taskId, exception);
            return false;
        }
        if (currentLock == null || StringUtils.isBlank(currentLock.getBatchId())) {
            return false;
        }
        DqlRecoveryBatchDto ownerBatch;
        try {
            ownerBatch = batchRepository.findByBatchId(currentLock.getBatchId());
        } catch (RuntimeException exception) {
            log.warn("DLQ recovery stale lock batch lookup failed, taskId={}, batchId={}",
                    taskId, currentLock.getBatchId(), exception);
            return false;
        }
        if (ownerBatch == null || !StringUtils.equals(taskId, ownerBatch.getTaskId())) {
            return false;
        }
        DqlRecoveryBatchStatusEnum ownerStatus = DqlRecoveryBatchStatusEnum.parse(ownerBatch.getStatus());
        if (!isTerminal(ownerStatus)) {
            if (!reconcileFinishedBatch(ownerBatch)) {
                return false;
            }
            return taskLockRepository.tryAcquire(taskId, batchId, leaseSeconds);
        }
        try {
            taskLockRepository.release(taskId, currentLock.getBatchId());
        } catch (RuntimeException exception) {
            log.warn("DLQ recovery finished task lock release failed, taskId={}, batchId={}",
                    taskId, currentLock.getBatchId(), exception);
            return false;
        }
        return taskLockRepository.tryAcquire(taskId, batchId, leaseSeconds);
    }

    /**
     * Repairs a batch whose event counters already account for every selected
     * event. This is used by preview/start as a compatibility repair for
     * batches created before the finish-callback ordering fix, and also closes
     * the gap when the finish callback was lost altogether.
     */
    private boolean reconcileFinishedBatch(DqlRecoveryBatchDto batch) {
        if (batch == null) {
            return false;
        }
        DqlRecoveryBatchStatusEnum currentStatus = DqlRecoveryBatchStatusEnum.parse(batch.getStatus());
        if (isTerminal(currentStatus)) {
            return true;
        }
        DqlRecoveryBatchStatusEnum status = reconciledStatus(batch);
        if (status == null) {
            return false;
        }
        boolean finished;
        try {
            finished = batchRepository.finishReconciled(
                    batch.getBatchId(), status,
                    batch.getSelectedCount(), batch.getSuccessCount(),
                    batch.getFailedCount(), batch.getSkippedCount(), batch.getMessage());
        } catch (RuntimeException exception) {
            log.warn("DLQ recovery completed batch reconciliation failed, batchId={}",
                    batch.getBatchId(), exception);
            return false;
        }
        if (!finished) {
            DqlRecoveryBatchDto latest;
            try {
                latest = batchRepository.findByBatchId(batch.getBatchId());
            } catch (RuntimeException exception) {
                log.warn("DLQ recovery completed batch recheck failed, batchId={}",
                        batch.getBatchId(), exception);
                return false;
            }
            if (latest == null || !isTerminal(DqlRecoveryBatchStatusEnum.parse(latest.getStatus()))) {
                return false;
            }
            batch = latest;
        }
        batch.setStatus(status.name());
        batch.setFinishedAt(new Date());
        batch.setFinishRequested(false);
        releaseTaskLock(batch);
        return true;
    }

    private DqlRuntimeConfig runtimeConfig() {
        SettingsService source = settingsService;
        return DqlRuntimeConfig.from(key -> {
            if (source == null) {
                return null;
            }
            try {
                Settings setting = source.getByKey(key);
                if (setting == null) {
                    return null;
                }
                Object value = setting.getValue() == null ? setting.getDefault_value() : setting.getValue();
                return value == null ? null : String.valueOf(value);
            } catch (RuntimeException exception) {
                return null;
            }
        });
    }

    private void releaseTaskLock(DqlRecoveryBatchDto batch) {
        if (batch != null) {
            releaseTaskLock(batch.getTaskId(), batch.getBatchId());
        }
    }

    private void releaseTaskLock(String taskId, String batchId) {
        if (taskLockRepository != null) {
            try {
                taskLockRepository.release(taskId, batchId);
            } catch (RuntimeException exception) {
                // Keep terminal callbacks idempotent after the batch state has
                // already been persisted. A repeated callback or a later
                // recovery start can retry this owner-specific cleanup.
                log.warn("DLQ recovery task lock release failed, taskId={}, batchId={}",
                        taskId, batchId, exception);
            }
        }
    }

    private DqlRecoveryBatchStatusEnum reconciledStatus(DqlRecoveryBatchDto batch) {
        if (batch == null
                || batch.getSelectedCount() == null
                || batch.getSuccessCount() == null
                || batch.getFailedCount() == null
                || batch.getSkippedCount() == null
                || batch.getSelectedCount() <= 0
                || batch.getSuccessCount() < 0
                || batch.getFailedCount() < 0
                || batch.getSkippedCount() < 0
                || batch.getSelectedCount()
                != batch.getSuccessCount() + batch.getFailedCount() + batch.getSkippedCount()) {
            return null;
        }
        return batch.getFailedCount() == 0 && batch.getSkippedCount() == 0
                ? DqlRecoveryBatchStatusEnum.SUCCESS
                : DqlRecoveryBatchStatusEnum.PARTIAL_FAILED;
    }

    private Map<String, DqlEventStatusEnum> originalEventStatuses(List<DqlEventDto> events) {
        return events.stream()
                .filter(event -> StringUtils.isNotBlank(event.getEventId()))
                .map(event -> {
                    DqlEventStatusEnum status = DqlEventStatusEnum.parse(event.getStatus());
                    return status == null ? null : Map.entry(event.getEventId(), status);
                })
                .filter(java.util.Objects::nonNull)
                .filter(entry -> entry.getValue() != null && entry.getValue().reprocessable())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (first, ignored) -> first, LinkedHashMap::new));
    }

    private record RecoveryTaskContext(TaskDto task, boolean agentAvailable) {
        private PreviewMessage taskRecoveryReason(DqlEventDto event) {
            String versionReason = taskVersionReason(event);
            if (versionReason != null) {
                return localizedPreviewMessage(versionReason);
            }
            if (!agentAvailable) {
                return localizedPreviewMessage(PREVIEW_AGENT_UNAVAILABLE_MESSAGE);
            }
            return null;
        }

        private String taskVersionReason(DqlEventDto event) {
            if (task.getVersion() == null || task.getVersion() < 1L) {
                return PREVIEW_CURRENT_TASK_VERSION_UNAVAILABLE_MESSAGE;
            }
            if (event.getTaskVersion() == null || event.getTaskVersion() < 1L) {
                return PREVIEW_EVENT_TASK_VERSION_UNAVAILABLE_MESSAGE;
            }
            if (!task.getVersion().equals(event.getTaskVersion())) {
                return PREVIEW_TASK_VERSION_CHANGED_MESSAGE;
            }
            return null;
        }
    }

    private enum StatusSyncTaskLookupState {
        FOUND,
        NOT_FOUND,
        UNAVAILABLE
    }

    private record StatusSyncTaskLookup(StatusSyncTaskLookupState state, TaskDto task) {
    }

    private record PreviewMessage(String code, String message) {
    }
}
