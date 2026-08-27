package com.tapdata.tm.dql.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.DqlRecoveryAttemptResultEnum;
import com.tapdata.tm.dql.DqlRecoveryBatchStatusEnum;
import com.tapdata.tm.dql.DqlRecoveryReportTypeEnum;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import com.tapdata.tm.dql.repository.DqlEventRepository;
import com.tapdata.tm.dql.repository.DqlRecoveryBatchRepository;
import com.tapdata.tm.dql.vo.DqlEventDetailVo;
import com.tapdata.tm.dql.vo.DqlRecoveryPreviewVo;
import com.tapdata.tm.dql.vo.DqlRecoveryRequestVo;
import com.tapdata.tm.dql.vo.DqlRecoveryResultReportVo;
import com.tapdata.tm.messagequeue.service.MessageQueueServiceImpl;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
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
public class DqlRecoveryBatchService {
    private static final int DEFAULT_BATCH_MAX_SIZE = 200;
    private static final String[] TASK_FIELDS = {"name", "status", "version", "agentId"};

    private final DqlEventRepository eventRepository;
    private final DqlRecoveryBatchRepository batchRepository;
    private final DqlEventPermissionService permissionService;
    private final MessageQueueServiceImpl messageQueueService;
    private final DqlEventAlarmService alarmService;
    private final TaskService taskService;
    private final WorkerService workerService;
    private final DqlEventWebMapper webMapper;

    public DqlRecoveryBatchService(DqlEventRepository eventRepository,
                                   DqlRecoveryBatchRepository batchRepository,
                                   DqlEventPermissionService permissionService,
                                   MessageQueueServiceImpl messageQueueService,
                                   DqlEventAlarmService alarmService) {
        this(eventRepository, batchRepository, permissionService, messageQueueService, alarmService, null, null);
    }

    @Autowired
    public DqlRecoveryBatchService(DqlEventRepository eventRepository,
                                   DqlRecoveryBatchRepository batchRepository,
                                   DqlEventPermissionService permissionService,
                                   MessageQueueServiceImpl messageQueueService,
                                   DqlEventAlarmService alarmService,
                                   TaskService taskService,
                                   WorkerService workerService) {
        this.eventRepository = eventRepository;
        this.batchRepository = batchRepository;
        this.permissionService = permissionService;
        this.messageQueueService = messageQueueService;
        this.alarmService = alarmService;
        this.taskService = taskService;
        this.workerService = workerService;
        this.webMapper = new DqlEventWebMapper();
    }

    public DqlRecoveryPreviewVo preview(DqlRecoveryRequestVo request, UserDetail user) {
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
        boolean batchSizeExceeded = eventIds.size() > DEFAULT_BATCH_MAX_SIZE;
        boolean activeBatchExists = !events.isEmpty()
                && batchRepository.findActiveByTaskId(events.get(0).getTaskId()) != null;

        Map<String, DqlEventDto> eventMap = events.stream().collect(Collectors.toMap(DqlEventDto::getEventId, event -> event, (a, b) -> a, LinkedHashMap::new));
        Map<String, String> blockedReasons = new LinkedHashMap<>();
        for (String eventId : eventIds) {
            DqlEventDto event = eventMap.get(eventId);
            if (event == null) {
                blockedReasons.put(eventId, "event not found");
            } else {
                String reason = previewBlockedReason(event, taskContext, batchSizeExceeded, activeBatchExists);
                if (reason != null) {
                    blockedReasons.put(eventId, reason);
                }
            }
        }

        for (String eventId : eventIds) {
            if (blockedReasons.containsKey(eventId)) {
                preview.getBlockedEvents().add(blocked(eventId, blockedReasons.get(eventId), eventMap.get(eventId)));
            }
        }

        events.stream()
                .filter(event -> !blockedReasons.containsKey(event.getEventId()))
                .sorted(Comparator.comparing(DqlEventDto::getEventTime, Comparator.nullsLast(Date::compareTo))
                        .thenComparing(DqlEventDto::getCaptureSeq, Comparator.nullsLast(Long::compareTo))
                        .thenComparing(DqlEventDto::getEventId))
                .map(this::orderedEvent)
                .forEach(preview.getOrderedEvents()::add);
        preview.setCanSubmit(preview.getBlockedEvents().isEmpty() && preview.getOrderedEvents().size() == eventIds.size());
        if (!preview.isCanSubmit()) {
            preview.setMessage("Some selected events cannot be reprocessed");
        }
        return preview;
    }

    public DqlRecoveryBatchDto start(DqlRecoveryRequestVo request, UserDetail user) {
        requireEventIds(request);
        if (!Boolean.TRUE.equals(request.getConfirm())) {
            throw new BizException("IllegalArgument", "confirm");
        }
        DqlRecoveryPreviewVo preview = preview(request, user);
        if (!preview.isCanSubmit()) {
            throw new BizException("DqlRecovery.EventNotReprocessable", preview.getMessage());
        }
        List<String> orderedEventIds = preview.getOrderedEvents().stream()
                .map(DqlRecoveryPreviewVo.OrderedEvent::getEventId)
                .toList();
        List<DqlEventDto> events = eventRepository.findByEventIds(orderedEventIds);
        RecoveryTaskContext taskContext = strictRecoveryValidation() ? loadTaskContext(preview.getTaskId()) : null;
        DqlRecoveryBatchDto batch = new DqlRecoveryBatchDto();
        batch.setBatchId(buildBatchId());
        batch.setTaskId(preview.getTaskId());
        batch.setTaskName(preview.getTaskName());
        batch.setTaskStatusBefore(taskContext == null ? null : taskContext.task().getStatus());
        batch.setTaskVersion(taskContext == null ? null : taskContext.task().getVersion());
        batch.setAgentId(taskContext == null
                ? (events.isEmpty() ? null : events.get(0).getAgentId())
                : taskContext.task().getAgentId());
        batch.setEventIds(requireEventIds(request));
        batch.setOrderedEventIds(orderedEventIds);
        batch.setOperatorId(user == null ? null : user.getUserId());
        batch.setOperatorName(user == null ? null : user.getUsername());
        batch.setStatus(DqlRecoveryBatchStatusEnum.CREATED.name());
        batch.setSelectedCount(orderedEventIds.size());
        batch.setSuccessCount(0);
        batch.setFailedCount(0);
        batch.setSkippedCount(0);
        batch = batchRepository.create(batch);

        long locked = eventRepository.lockEvents(orderedEventIds, batch.getBatchId());
        if (locked != orderedEventIds.size()) {
            eventRepository.releaseBatchLocks(batch.getBatchId(), DqlEventStatusEnum.PENDING);
            batchRepository.finish(batch.getBatchId(), DqlRecoveryBatchStatusEnum.FAILED, "Failed to lock selected events");
            throw new BizException("DqlRecovery.EventLockFailed", batch.getBatchId());
        }
        try {
            dispatch(batch);
            batchRepository.updateStatus(batch.getBatchId(), DqlRecoveryBatchStatusEnum.DISPATCHED, null);
            batch.setStatus(DqlRecoveryBatchStatusEnum.DISPATCHED.name());
            return batch;
        } catch (RuntimeException e) {
            eventRepository.releaseBatchLocks(batch.getBatchId(), DqlEventStatusEnum.PENDING);
            batchRepository.finish(batch.getBatchId(), DqlRecoveryBatchStatusEnum.FAILED, e.getMessage());
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
            case BATCH_STARTED -> batchRepository.markRunning(report.getBatchId());
            case EVENT_RESULT -> handleEventResult(batch, report);
            case BATCH_FINISHED -> finishBatch(batch, report);
            case BATCH_FAILED -> failBatch(batch, report.getMessage());
            case EVENT_STARTED -> {
            }
            default -> throw new BizException("IllegalArgument", "type");
        }
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
        return batch;
    }

    private void checkMenuPermission(UserDetail user) {
        if (permissionService != null) {
            permissionService.checkMenuVisible(user);
        }
    }

    private void handleEventResult(DqlRecoveryBatchDto batch, DqlRecoveryResultReportVo report) {
        if (StringUtils.isBlank(report.getEventId()) || batch.getEventIds() == null || !batch.getEventIds().contains(report.getEventId())) {
            throw new BizException("IllegalArgument", "eventId");
        }
        DqlRecoveryAttemptDto attempt = attempt(batch, report);
        DqlRecoveryAttemptResultEnum result = Optional.ofNullable(DqlRecoveryAttemptResultEnum.parse(report.getResult()))
                .orElse(DqlRecoveryAttemptResultEnum.FAILED);
        if (result == DqlRecoveryAttemptResultEnum.SUCCESS) {
            if (eventRepository.completeEvent(report.getEventId(), batch.getBatchId(), attempt)) {
                batchRepository.increaseSuccess(batch.getBatchId());
            }
        } else if (result == DqlRecoveryAttemptResultEnum.SKIPPED) {
            if (eventRepository.failEvent(report.getEventId(), batch.getBatchId(), attempt)) {
                batchRepository.increaseSkipped(batch.getBatchId());
            }
        } else {
            if (eventRepository.failEvent(report.getEventId(), batch.getBatchId(), attempt)) {
                batchRepository.increaseFailed(batch.getBatchId());
                alarmService.notifyRecoveryFailed(batch);
            }
        }
    }

    private void finishBatch(DqlRecoveryBatchDto batch, DqlRecoveryResultReportVo report) {
        int selected = Optional.ofNullable(batch.getSelectedCount()).orElse(0);
        int failed = Optional.ofNullable(batch.getFailedCount()).orElse(0);
        int skipped = Optional.ofNullable(batch.getSkippedCount()).orElse(0);
        DqlRecoveryBatchStatusEnum status = failed == 0 && skipped == 0
                ? DqlRecoveryBatchStatusEnum.SUCCESS
                : DqlRecoveryBatchStatusEnum.PARTIAL_FAILED;
        batchRepository.finish(batch.getBatchId(), status, report.getMessage());
        if (selected > 0 && status == DqlRecoveryBatchStatusEnum.PARTIAL_FAILED) {
            alarmService.notifyBatchPartialFailed(batch);
        }
    }

    private void failBatch(DqlRecoveryBatchDto batch, String message) {
        eventRepository.releaseBatchLocks(batch.getBatchId(), DqlEventStatusEnum.RECOVERY_FAILED);
        batchRepository.finish(batch.getBatchId(), DqlRecoveryBatchStatusEnum.FAILED, message);
        alarmService.notifyRecoveryFailed(batch);
    }

    private DqlRecoveryAttemptDto attempt(DqlRecoveryBatchDto batch, DqlRecoveryResultReportVo report) {
        DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
        attempt.setAttemptId(report.getAttemptId());
        attempt.setBatchId(batch.getBatchId());
        attempt.setOperatorId(batch.getOperatorId());
        attempt.setOperatorName(batch.getOperatorName());
        attempt.setTaskVersion(batch.getTaskVersion());
        attempt.setStartedAt(report.getStartedAt() == null ? new Date() : new Date(report.getStartedAt()));
        attempt.setFinishedAt(report.getFinishedAt() == null ? new Date() : new Date(report.getFinishedAt()));
        attempt.setResult(Optional.ofNullable(report.getResult()).orElse(DqlRecoveryAttemptResultEnum.FAILED.name()));
        attempt.setMessage(report.getMessage());
        attempt.setErrorCode(report.getErrorCode());
        attempt.setErrorDetails(report.getErrorDetails());
        return attempt;
    }

    private void dispatch(DqlRecoveryBatchDto batch) {
        if (messageQueueService == null || StringUtils.isBlank(batch.getAgentId())) {
            return;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "dqlRecovery");
        payload.put("taskId", batch.getTaskId());
        payload.put("batchId", batch.getBatchId());
        payload.put("eventIds", batch.getOrderedEventIds());
        messageQueueService.sendPipeMessage(payload, "tm", batch.getAgentId());
    }

    private boolean isReprocessable(DqlEventDto event) {
        DqlEventStatusEnum status = DqlEventStatusEnum.parse(event.getStatus());
        return status != null && status.reprocessable() && !Boolean.FALSE.equals(event.getPayloadComplete());
    }

    private String previewBlockedReason(DqlEventDto event,
                                        RecoveryTaskContext taskContext,
                                        boolean batchSizeExceeded,
                                        boolean activeBatchExists) {
        if (!isReprocessable(event)) {
            return blockedReason(event);
        }
        if (strictRecoveryValidation()) {
            if (Boolean.TRUE.equals(event.getEventKeyMissing()) || StringUtils.isBlank(event.getRecordIdentity())) {
                return "event has no business key";
            }
            String taskReason = taskContext.taskRecoveryReason(event);
            if (taskReason != null) {
                return taskReason;
            }
        }
        if (batchSizeExceeded) {
            return "recovery batch size exceeds " + DEFAULT_BATCH_MAX_SIZE;
        }
        if (activeBatchExists) {
            return "an active recovery batch already exists";
        }
        return null;
    }

    private String blockedReason(DqlEventDto event) {
        DqlEventStatusEnum status = DqlEventStatusEnum.parse(event.getStatus());
        if (status == null || !status.reprocessable()) {
            return "status " + event.getStatus() + " is not reprocessable";
        }
        if (Boolean.FALSE.equals(event.getPayloadComplete())) {
            return "payload is incomplete";
        }
        return "event is not reprocessable";
    }

    private boolean strictRecoveryValidation() {
        return taskService != null;
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

    private DqlRecoveryPreviewVo.BlockedEvent blocked(String eventId, String message, DqlEventDto event) {
        DqlRecoveryPreviewVo.BlockedEvent blocked = new DqlRecoveryPreviewVo.BlockedEvent();
        blocked.setEventId(eventId);
        blocked.setMessage(message);
        if (event != null) {
            blocked.setSourceTable(event.getSourceTable());
            blocked.setTargetTable(event.getTargetTable());
            blocked.setDmlType(event.getDmlType());
            blocked.setEventTime(event.getEventTime());
            blocked.setCaptureSeq(event.getCaptureSeq());
        }
        return blocked;
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

    private record RecoveryTaskContext(TaskDto task, boolean agentAvailable) {
        private String taskRecoveryReason(DqlEventDto event) {
            if (!TaskDto.STATUS_RUNNING.equals(task.getStatus()) && !TaskDto.STATUS_STOP.equals(task.getStatus())) {
                return "task status " + task.getStatus() + " does not support recovery";
            }
            if (task.getVersion() == null || event.getTaskVersion() == null) {
                return "task version is unavailable";
            }
            if (!task.getVersion().equals(event.getTaskVersion())) {
                return "task version has changed";
            }
            if (!agentAvailable) {
                return "agent is not available";
            }
            return null;
        }
    }
}
