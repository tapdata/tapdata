package com.tapdata.tm.dql.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.DqlErrorTypeEnum;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.repository.DqlEventRepository;
import com.tapdata.tm.dql.repository.DqlRecoveryBatchRepository;
import com.tapdata.tm.dql.vo.DqlEventDetailVo;
import com.tapdata.tm.dql.vo.DqlEventListVo;
import com.tapdata.tm.dql.vo.DqlEventQueryVo;
import com.tapdata.tm.dql.vo.DqlEventReportResultVo;
import com.tapdata.tm.dql.vo.DqlEventReportVo;
import com.tapdata.tm.dql.vo.DqlEventSummaryVo;
import com.tapdata.tm.dql.vo.DqlRecordSuccessReportResultVo;
import com.tapdata.tm.dql.vo.DqlRecordSuccessReportVo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Service
public class DqlEventService {
    static final String OVERWRITE_RISK_MESSAGE = "该事件异常后，同记录后续存在成功执行的事件，继续重放存在数据覆盖风险，请谨慎操作";
    private static final int SAVE_FAILURE_REASON_MAX_LENGTH = 512;
    private static final String[] SENSITIVE_FAILURE_TOKENS = {
            "password", "passwd", "secret", "token", "access_token", "authorization", "credential",
            "apikey", "payload", "eventkey", "recordidentity"
    };

    private final DqlEventRepository eventRepository;
    private final DqlEventAlarmService alarmService;
    private final DqlEventPermissionService permissionService;
    private final DqlRecoveryBatchRepository batchRepository;
    private final DqlReportValidationService reportValidationService;
    private final DqlEventIdentityService identityService;
    private final DqlEventWebMapper webMapper;

    public DqlEventService(DqlEventRepository eventRepository, DqlEventAlarmService alarmService) {
        this(eventRepository, alarmService, null, null, new DqlReportValidationService(), new DqlEventIdentityService());
    }

    public DqlEventService(DqlEventRepository eventRepository,
                           DqlEventAlarmService alarmService,
                           DqlEventPermissionService permissionService) {
        this(eventRepository, alarmService, permissionService, null, new DqlReportValidationService(), new DqlEventIdentityService());
    }

    public DqlEventService(DqlEventRepository eventRepository,
                           DqlEventAlarmService alarmService,
                           DqlEventPermissionService permissionService,
                           DqlRecoveryBatchRepository batchRepository) {
        this(eventRepository, alarmService, permissionService, batchRepository, new DqlReportValidationService(), new DqlEventIdentityService());
    }

    public DqlEventService(DqlEventRepository eventRepository,
                           DqlEventAlarmService alarmService,
                           DqlEventPermissionService permissionService,
                           DqlRecoveryBatchRepository batchRepository,
                           DqlReportValidationService reportValidationService) {
        this(eventRepository, alarmService, permissionService, batchRepository, reportValidationService, new DqlEventIdentityService());
    }

    @Autowired
    public DqlEventService(DqlEventRepository eventRepository,
                           DqlEventAlarmService alarmService,
                           DqlEventPermissionService permissionService,
                           DqlRecoveryBatchRepository batchRepository,
                           DqlReportValidationService reportValidationService,
                           DqlEventIdentityService identityService) {
        this.eventRepository = eventRepository;
        this.alarmService = alarmService;
        this.permissionService = permissionService;
        this.batchRepository = batchRepository;
        this.reportValidationService = reportValidationService;
        this.identityService = identityService;
        this.webMapper = new DqlEventWebMapper();
    }

    public DqlEventReportResultVo report(String taskId, DqlEventReportVo report) {
        if (report == null) {
            throw new BizException("IllegalArgument", "vo");
        }
        if (StringUtils.isBlank(taskId)) {
            throw new BizException("IllegalArgument", "taskId");
        }
        DqlReportValidationService.ValidationResult validationResult = reportValidationService.validateAndSecure(taskId, report);
        identityService.fillIdentities(taskId, report);
        DqlEventDto duplicate = eventRepository.findDuplicate(taskId, report);
        if (duplicate != null) {
            return reportResult(duplicate, true);
        }

        Long captureSeq = Optional.ofNullable(report.getCaptureSeq()).orElseGet(() -> eventRepository.nextCaptureSeq(taskId));
        DqlEventDto dto = convert(taskId, report, captureSeq, validationResult);
        DqlEventDto saved = persist(taskId, dto);
        boolean duplicateAfterUpsert = !Objects.equals(dto.getEventId(), saved.getEventId());
        if (!duplicateAfterUpsert) {
            alarmService.notifyEventCreated(saved);
        }
        return reportResult(saved, duplicateAfterUpsert);
    }

    /**
     * Persists a report without allowing a database failure (or an empty repository result) to be
     * interpreted as a successful report.  The Engine uses the error response as the signal that
     * it must not skip the current record.
     */
    private DqlEventDto persist(String taskId, DqlEventDto dto) {
        try {
            DqlEventDto saved = eventRepository.upsert(dto);
            if (saved == null) {
                throw new IllegalStateException("DQL event persistence returned no event");
            }
            return saved;
        } catch (RuntimeException cause) {
            notifySaveFailed(taskId, cause);
            throw new BizException(cause);
        }
    }

    private void notifySaveFailed(String taskId, RuntimeException cause) {
        if (alarmService == null) {
            return;
        }
        try {
            alarmService.notifySaveFailed(taskId, saveFailureReason(cause));
        } catch (RuntimeException ignored) {
            // An alarm failure must not mask the persistence failure returned to Engine.
        }
    }

    private String saveFailureReason(RuntimeException cause) {
        String message = cause.getMessage();
        if (StringUtils.isBlank(message) || StringUtils.containsAnyIgnoreCase(message, SENSITIVE_FAILURE_TOKENS)) {
            return cause.getClass().getSimpleName();
        }
        String reason = cause.getClass().getSimpleName() + ": " + message;
        return reason.length() <= SAVE_FAILURE_REASON_MAX_LENGTH
                ? reason
                : reason.substring(0, SAVE_FAILURE_REASON_MAX_LENGTH);
    }

    public Page<DqlEventListVo> page(DqlEventQueryVo query, UserDetail user) {
        checkQueryPermission(query, user);
        Page<DqlEventDto> page = eventRepository.page(query);
        if (page == null) {
            return Page.empty();
        }
        List<DqlEventListVo> items = page.getItems() == null
                ? List.of()
                : page.getItems().stream().map(webMapper::toList).toList();
        return Page.page(items, page.getTotal());
    }

    public DqlEventDetailVo detail(String eventId, UserDetail user) {
        DqlEventDto event = eventRepository.findByEventId(eventId);
        if (event == null) {
            throw new BizException("DqlEvent.NotFound", eventId);
        }
        checkEventPermission(event, user);
        DqlEventDetailVo detail = webMapper.toDetail(event);
        if (StringUtils.isNotBlank(event.getCurrentBatchId()) && batchRepository != null) {
            detail.setCurrentBatch(batchRepository.findByBatchId(event.getCurrentBatchId()));
        }
        return detail;
    }

    public DqlEventSummaryVo summary(DqlEventQueryVo query, UserDetail user) {
        DqlEventQueryVo summaryQuery = summaryQuery(query);
        checkQueryPermission(summaryQuery, user);
        DqlEventSummaryVo summary = new DqlEventSummaryVo();
        summary.setTotal(eventRepository.count(summaryQuery));
        summary.setPending(eventRepository.countByStatus(summaryQuery, DqlEventStatusEnum.PENDING));
        summary.setReprocessing(eventRepository.countByStatus(summaryQuery, DqlEventStatusEnum.REPROCESSING));
        summary.setRecovered(eventRepository.countByStatus(summaryQuery, DqlEventStatusEnum.RECOVERED));
        summary.setRecoveryFailed(eventRepository.countByStatus(summaryQuery, DqlEventStatusEnum.RECOVERY_FAILED));
        summary.setNotReprocessable(eventRepository.countByStatus(summaryQuery, DqlEventStatusEnum.NOT_REPROCESSABLE));
        return summary;
    }

    private DqlEventQueryVo summaryQuery(DqlEventQueryVo query) {
        DqlEventQueryVo scoped = new DqlEventQueryVo();
        if (query != null) {
            BeanUtils.copyProperties(query, scoped);
        }
        scoped.setStatus(null);
        scoped.setSkip(0L);
        scoped.setLimit(0);
        scoped.setOrder(null);
        return scoped;
    }

    /**
     * Marks the latest unresolved DLQ event for the same business record when Engine reports a later successful write.
     */
    public DqlRecordSuccessReportResultVo reportRecordSuccess(String taskId, DqlRecordSuccessReportVo report) {
        if (report == null) {
            throw new BizException("IllegalArgument", "vo");
        }
        if (StringUtils.isBlank(taskId)) {
            throw new BizException("IllegalArgument", "taskId");
        }
        identityService.fillRecordIdentity(report);
        DqlEventDto marked = eventRepository.markLaterSuccess(taskId, report, OVERWRITE_RISK_MESSAGE);
        DqlRecordSuccessReportResultVo result = new DqlRecordSuccessReportResultVo();
        result.setMarked(marked != null);
        result.setRecordIdentity(report.getRecordIdentity());
        if (marked != null) {
            result.setEventId(marked.getEventId());
            result.setOverwriteRiskMessage(marked.getOverwriteRiskMessage());
        }
        return result;
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

    private DqlEventDto convert(String taskId,
                                DqlEventReportVo report,
                                Long captureSeq,
                                DqlReportValidationService.ValidationResult validationResult) {
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
        dto.setTargetNodeId(report.getTargetNodeId());
        dto.setTargetNodeName(report.getTargetNodeName());
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
        dto.setRecordIdentity(report.getRecordIdentity());
        dto.setRecordIdentityType(report.getRecordIdentityType());
        dto.setRecordIdentityFields(report.getRecordIdentityFields());
        dto.setPayloadFormat(Optional.ofNullable(report.getPayloadFormat()).orElse("tap-record-event-json-v1"));
        dto.setPayloadData(report.getPayloadData());
        dto.setPayloadHash(report.getPayloadHash());
        dto.setPayloadSize(report.getPayloadSize());
        dto.setPayloadComplete(Optional.ofNullable(report.getPayloadComplete()).orElse(true));
        dto.setPayloadPreview(report.getPayloadPreview());
        dto.setPayloadPreviewTruncated(Optional.ofNullable(report.getPayloadPreviewTruncated()).orElse(false));
        DqlErrorTypeEnum errorType = DqlErrorTypeEnum.parse(report.getErrorType());
        dto.setErrorType(errorType == null ? report.getErrorType() : errorType.name());
        dto.setErrorCode(report.getErrorCode());
        dto.setExceptionScope(report.getExceptionScope());
        dto.setRouteDecision(report.getRouteDecision());
        dto.setClassificationReason(report.getClassificationReason());
        dto.setClassificationConfidence(report.getClassificationConfidence());
        dto.setErrorDetails(report.getErrorDetails());
        dto.setErrorDetailsTruncated(validationResult.errorDetailsTruncated());
        dto.setRawErrorRef(report.getRawErrorRef());
        dto.setStatus(Boolean.FALSE.equals(dto.getPayloadComplete())
                ? DqlEventStatusEnum.NOT_REPROCESSABLE.name()
                : DqlEventStatusEnum.PENDING.name());
        dto.setRecoveryCount(0);
        dto.setOverwriteRisk(false);
        dto.setCreated(now);
        dto.setUpdated(now);
        dto.setTtlAt(now);
        return dto;
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

}
