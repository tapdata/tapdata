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
import com.tapdata.tm.dql.vo.DqlRecoveryPreviewVo;
import com.tapdata.tm.dql.vo.DqlRecoveryRequestVo;
import com.tapdata.tm.dql.vo.DqlRecoveryResultReportVo;
import com.tapdata.tm.messagequeue.service.MessageQueueServiceImpl;
import org.apache.commons.lang3.StringUtils;
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
    private final DqlEventRepository eventRepository;
    private final DqlRecoveryBatchRepository batchRepository;
    private final DqlEventPermissionService permissionService;
    private final MessageQueueServiceImpl messageQueueService;
    private final DqlEventAlarmService alarmService;

    public DqlRecoveryBatchService(DqlEventRepository eventRepository,
                                   DqlRecoveryBatchRepository batchRepository,
                                   DqlEventPermissionService permissionService,
                                   MessageQueueServiceImpl messageQueueService,
                                   DqlEventAlarmService alarmService) {
        this.eventRepository = eventRepository;
        this.batchRepository = batchRepository;
        this.permissionService = permissionService;
        this.messageQueueService = messageQueueService;
        this.alarmService = alarmService;
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

        Map<String, DqlEventDto> eventMap = events.stream().collect(Collectors.toMap(DqlEventDto::getEventId, event -> event, (a, b) -> a, LinkedHashMap::new));
        for (String eventId : eventIds) {
            DqlEventDto event = eventMap.get(eventId);
            if (event == null) {
                preview.getBlockedEvents().add(blocked(eventId, "event not found", null));
            } else if (!isReprocessable(event)) {
                preview.getBlockedEvents().add(blocked(eventId, blockedReason(event), event));
            }
        }

        events.stream()
                .filter(this::isReprocessable)
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
        DqlRecoveryBatchDto batch = new DqlRecoveryBatchDto();
        batch.setBatchId(buildBatchId());
        batch.setTaskId(preview.getTaskId());
        batch.setTaskName(preview.getTaskName());
        batch.setAgentId(events.isEmpty() ? null : events.get(0).getAgentId());
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
        DqlRecoveryPreviewVo.OrderedEvent ordered = new DqlRecoveryPreviewVo.OrderedEvent();
        ordered.setEventId(event.getEventId());
        ordered.setEventTime(event.getEventTime());
        ordered.setCaptureSeq(event.getCaptureSeq());
        ordered.setDmlType(event.getDmlType());
        ordered.setSourceTable(event.getSourceTable());
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
}
