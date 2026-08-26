package com.tapdata.tm.dql.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto;
import com.tapdata.tm.dql.repository.DqlEventRepository;
import com.tapdata.tm.dql.repository.DqlRecoveryBatchRepository;
import com.tapdata.tm.dql.vo.DqlEventDetailVo;
import com.tapdata.tm.dql.vo.DqlEventQueryVo;
import com.tapdata.tm.dql.vo.DqlEventReportResultVo;
import com.tapdata.tm.dql.vo.DqlEventReportVo;
import com.tapdata.tm.dql.vo.DqlEventSummaryVo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Service
public class DqlEventService {
    private static final int ERROR_DETAILS_MAX_LENGTH = 4000;

    private final DqlEventRepository eventRepository;
    private final DqlEventAlarmService alarmService;
    private final DqlEventPermissionService permissionService;
    private final DqlRecoveryBatchRepository batchRepository;

    public DqlEventService(DqlEventRepository eventRepository, DqlEventAlarmService alarmService) {
        this(eventRepository, alarmService, null, null);
    }

    public DqlEventService(DqlEventRepository eventRepository,
                           DqlEventAlarmService alarmService,
                           DqlEventPermissionService permissionService) {
        this(eventRepository, alarmService, permissionService, null);
    }

    @Autowired
    public DqlEventService(DqlEventRepository eventRepository,
                           DqlEventAlarmService alarmService,
                           DqlEventPermissionService permissionService,
                           DqlRecoveryBatchRepository batchRepository) {
        this.eventRepository = eventRepository;
        this.alarmService = alarmService;
        this.permissionService = permissionService;
        this.batchRepository = batchRepository;
    }

    public DqlEventReportResultVo report(String taskId, DqlEventReportVo report) {
        if (report == null) {
            throw new BizException("IllegalArgument", "vo");
        }
        if (StringUtils.isBlank(taskId)) {
            throw new BizException("IllegalArgument", "taskId");
        }
        if (StringUtils.isBlank(report.getEventIdentity())) {
            report.setEventIdentity(generateIdentity(taskId, report));
        }
        DqlEventDto duplicate = eventRepository.findDuplicate(taskId, report);
        if (duplicate != null) {
            return reportResult(duplicate, true);
        }

        Long captureSeq = Optional.ofNullable(report.getCaptureSeq()).orElseGet(() -> eventRepository.nextCaptureSeq(taskId));
        DqlEventDto dto = convert(taskId, report, captureSeq);
        DqlEventDto saved = eventRepository.upsert(dto);
        alarmService.notifyEventCreated(saved);
        return reportResult(saved, false);
    }

    public Page<DqlEventDto> page(DqlEventQueryVo query, UserDetail user) {
        checkQueryPermission(query, user);
        Page<DqlEventDto> page = eventRepository.page(query);
        page.getItems().forEach(this::sanitizeListItem);
        return page;
    }

    public DqlEventDetailVo detail(String eventId, UserDetail user) {
        DqlEventDto event = eventRepository.findByEventId(eventId);
        if (event == null) {
            throw new BizException("DqlEvent.NotFound", eventId);
        }
        checkEventPermission(event, user);
        DqlEventDetailVo detail = new DqlEventDetailVo();
        BeanUtils.copyProperties(event, detail);
        detail.setPayloadData(null);
        detail.setRecoveryAttempts(lastAttempts(event.getRecoveryAttempts()));
        if (StringUtils.isNotBlank(event.getCurrentBatchId()) && batchRepository != null) {
            detail.setCurrentBatch(batchRepository.findByBatchId(event.getCurrentBatchId()));
        }
        return detail;
    }

    public DqlEventSummaryVo summary(DqlEventQueryVo query, UserDetail user) {
        checkQueryPermission(query, user);
        DqlEventSummaryVo summary = new DqlEventSummaryVo();
        summary.setTotal(eventRepository.count(query));
        summary.setPending(eventRepository.countByStatus(query, DqlEventStatusEnum.PENDING));
        summary.setReprocessing(eventRepository.countByStatus(query, DqlEventStatusEnum.REPROCESSING));
        summary.setRecovered(eventRepository.countByStatus(query, DqlEventStatusEnum.RECOVERED));
        summary.setRecoveryFailed(eventRepository.countByStatus(query, DqlEventStatusEnum.RECOVERY_FAILED));
        summary.setNotReprocessable(eventRepository.countByStatus(query, DqlEventStatusEnum.NOT_REPROCESSABLE));
        return summary;
    }

    private void checkQueryPermission(DqlEventQueryVo query, UserDetail user) {
        if (permissionService == null) {
            return;
        }
        permissionService.checkMenuVisible(user);
        if (query != null && StringUtils.isNotBlank(query.getTaskId())) {
            permissionService.checkTaskVisible(query.getTaskId(), user);
        }
    }

    private void checkEventPermission(DqlEventDto event, UserDetail user) {
        if (permissionService == null) {
            return;
        }
        permissionService.checkMenuVisible(user);
        permissionService.checkTaskVisible(event.getTaskId(), user);
    }

    private List<DqlRecoveryAttemptDto> lastAttempts(List<DqlRecoveryAttemptDto> attempts) {
        if (attempts == null || attempts.size() <= 20) {
            return attempts;
        }
        return new ArrayList<>(attempts.subList(attempts.size() - 20, attempts.size()));
    }

    private void sanitizeListItem(DqlEventDto event) {
        if (event == null) {
            return;
        }
        event.setPayloadData(null);
        event.setRecoveryAttempts(null);
    }

    private DqlEventDto convert(String taskId, DqlEventReportVo report, Long captureSeq) {
        Date now = new Date();
        DqlEventDto dto = new DqlEventDto();
        dto.setEventId(Optional.ofNullable(report.getEventId()).filter(StringUtils::isNotBlank).orElseGet(() -> buildEventId(taskId, captureSeq)));
        dto.setTaskId(taskId);
        dto.setTaskRecordId(report.getTaskRecordId());
        dto.setTaskName(report.getTaskName());
        dto.setTaskVersion(report.getTaskVersion());
        dto.setAgentId(report.getAgentId());
        dto.setSourceNodeId(report.getSourceNodeId());
        dto.setSourceNodeName(report.getSourceNodeName());
        dto.setFailedNodeId(report.getFailedNodeId());
        dto.setFailedNodeName(report.getFailedNodeName());
        dto.setFailedStage(report.getFailedStage());
        dto.setSourceTable(report.getSourceTable());
        dto.setTargetTable(report.getTargetTable());
        dto.setTableId(report.getTableId());
        dto.setDmlType(report.getDmlType());
        dto.setEventTime(new Date(Optional.ofNullable(report.getEventTime()).orElse(now.getTime())));
        dto.setCaptureSeq(captureSeq);
        dto.setFailedAt(now);
        dto.setEventKey(report.getEventKey());
        dto.setEventKeyMissing(Optional.ofNullable(report.getEventKeyMissing()).orElse(report.getEventKey() == null || report.getEventKey().isEmpty()));
        dto.setEventIdentity(report.getEventIdentity());
        dto.setPayloadFormat(Optional.ofNullable(report.getPayloadFormat()).orElse("tap-record-event-json-v1"));
        dto.setPayloadData(report.getPayloadData());
        dto.setPayloadHash(report.getPayloadHash());
        dto.setPayloadSize(report.getPayloadSize());
        dto.setPayloadComplete(Optional.ofNullable(report.getPayloadComplete()).orElse(true));
        dto.setPayloadPreview(report.getPayloadPreview());
        dto.setPayloadPreviewTruncated(Optional.ofNullable(report.getPayloadPreviewTruncated()).orElse(false));
        dto.setErrorType(report.getErrorType());
        dto.setErrorCode(report.getErrorCode());
        dto.setErrorDetails(truncateErrorDetails(report.getErrorDetails(), dto));
        dto.setRawErrorRef(report.getRawErrorRef());
        dto.setStatus(Boolean.FALSE.equals(dto.getPayloadComplete())
                ? DqlEventStatusEnum.NOT_REPROCESSABLE.name()
                : DqlEventStatusEnum.PENDING.name());
        dto.setRecoveryCount(0);
        dto.setCreated(now);
        dto.setUpdated(now);
        return dto;
    }

    private String truncateErrorDetails(String errorDetails, DqlEventDto dto) {
        if (errorDetails == null || errorDetails.length() <= ERROR_DETAILS_MAX_LENGTH) {
            dto.setErrorDetailsTruncated(false);
            return errorDetails;
        }
        dto.setErrorDetailsTruncated(true);
        return errorDetails.substring(0, ERROR_DETAILS_MAX_LENGTH);
    }

    private DqlEventReportResultVo reportResult(DqlEventDto event, boolean duplicate) {
        DqlEventReportResultVo result = new DqlEventReportResultVo();
        result.setEventId(event.getEventId());
        result.setStatus(event.getStatus());
        result.setDuplicate(duplicate);
        return result;
    }

    private String buildEventId(String taskId, Long captureSeq) {
        String shortId = taskId.length() <= 6 ? taskId : taskId.substring(0, 6);
        return String.format("DQL-%s-%06d", shortId, captureSeq);
    }

    private String generateIdentity(String taskId, DqlEventReportVo report) {
        String source = String.join("|",
                taskId,
                nullToEmpty(report.getTaskRecordId()),
                nullToEmpty(report.getTableId()),
                nullToEmpty(report.getDmlType()),
                String.valueOf(report.getEventTime()),
                nullToEmpty(report.getPayloadHash()),
                String.valueOf(report.getPayloadData()),
                nullToEmpty(report.getFailedNodeId())
        );
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(source.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder("sha256:");
            for (byte b : bytes) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new BizException("SystemError", e.getMessage());
        }
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
