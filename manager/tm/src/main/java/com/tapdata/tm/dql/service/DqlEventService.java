package com.tapdata.tm.dql.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
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
import com.tapdata.tm.dql.vo.DqlRecoveryPayloadVo;
import com.tapdata.tm.dql.vo.DqlStormGuardReportVo;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.service.TaskService;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    private final TaskService taskService;
    private final DqlEventWebMapper webMapper;

    public DqlEventService(DqlEventRepository eventRepository, DqlEventAlarmService alarmService) {
        this(eventRepository, alarmService, null, null, new DqlReportValidationService(), new DqlEventIdentityService(), null);
    }

    public DqlEventService(DqlEventRepository eventRepository,
                           DqlEventAlarmService alarmService,
                           DqlEventPermissionService permissionService) {
        this(eventRepository, alarmService, permissionService, null, new DqlReportValidationService(), new DqlEventIdentityService(), null);
    }

    public DqlEventService(DqlEventRepository eventRepository,
                           DqlEventAlarmService alarmService,
                           DqlEventPermissionService permissionService,
                           DqlRecoveryBatchRepository batchRepository) {
        this(eventRepository, alarmService, permissionService, batchRepository, new DqlReportValidationService(), new DqlEventIdentityService(), null);
    }

    public DqlEventService(DqlEventRepository eventRepository,
                           DqlEventAlarmService alarmService,
                           DqlEventPermissionService permissionService,
                           DqlRecoveryBatchRepository batchRepository,
                           DqlReportValidationService reportValidationService) {
        this(eventRepository, alarmService, permissionService, batchRepository, reportValidationService, new DqlEventIdentityService(), null);
    }

    public DqlEventService(DqlEventRepository eventRepository,
                           DqlEventAlarmService alarmService,
                           DqlEventPermissionService permissionService,
                           DqlRecoveryBatchRepository batchRepository,
                           DqlReportValidationService reportValidationService,
                           DqlEventIdentityService identityService) {
        this(eventRepository, alarmService, permissionService, batchRepository, reportValidationService, identityService, null);
    }

    @Autowired
    public DqlEventService(DqlEventRepository eventRepository,
                           DqlEventAlarmService alarmService,
                           DqlEventPermissionService permissionService,
                           DqlRecoveryBatchRepository batchRepository,
                           DqlReportValidationService reportValidationService,
                           DqlEventIdentityService identityService,
                           TaskService taskService) {
        this.eventRepository = eventRepository;
        this.alarmService = alarmService;
        this.permissionService = permissionService;
        this.batchRepository = batchRepository;
        this.reportValidationService = reportValidationService;
        this.identityService = identityService;
        this.taskService = taskService;
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
            notifySaveFailed(taskId, dto, cause);
            throw new BizException(cause);
        }
    }

    private void notifySaveFailed(String taskId, DqlEventDto event, RuntimeException cause) {
        if (alarmService == null) {
            return;
        }
        try {
            alarmService.notifySaveFailed(taskId, event, saveFailureReason(cause));
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
        DqlEventQueryResolution resolved = resolveQuery(query, user);
        Page<DqlEventDto> page = resolved.visibleTaskIds() == null
                ? eventRepository.page(resolved.query())
                : eventRepository.page(resolved.query(), resolved.visibleTaskIds());
        if (page == null) {
            return Page.empty();
        }
        Map<String, String> currentTaskNames = resolveCurrentTaskNames(page.getItems(), user);
        List<DqlEventListVo> items = page.getItems() == null
                ? List.of()
                : page.getItems().stream()
                        .map(event -> webMapper.toList(event, currentTaskName(event, currentTaskNames)))
                        .toList();
        return Page.page(items, page.getTotal());
    }

    public DqlEventDetailVo detail(String eventId, UserDetail user) {
        checkMenuPermission(user);
        DqlEventDto event = eventRepository.findByEventId(eventId);
        if (event == null) {
            throw new BizException("DqlEvent.NotFound", eventId);
        }
        checkEventTaskPermission(event, user);
        DqlEventDetailVo detail = webMapper.toDetail(event, resolveCurrentTaskName(event));
        if (StringUtils.isNotBlank(event.getCurrentBatchId()) && batchRepository != null) {
            detail.setCurrentBatch(batchRepository.findByBatchId(event.getCurrentBatchId()));
        }
        return detail;
    }

    /**
     * Returns only the immutable payload needed by Engine to replay a DQL event.
     * The endpoint deliberately avoids exposing the persistence DTO and its internal fields.
     */
    public DqlRecoveryPayloadVo recoveryPayload(String eventId, UserDetail user) {
        checkMenuPermission(user);
        DqlEventDto event = eventRepository.findByEventId(eventId);
        if (event == null) {
            throw new BizException("DqlEvent.NotFound", eventId);
        }
        checkEventTaskPermission(event, user);
        DqlRecoveryPayloadVo payload = new DqlRecoveryPayloadVo();
        payload.setSourceNodeId(event.getSourceNodeId());
        payload.setSourceNodeName(event.getSourceNodeName());
        payload.setFailedNodeId(event.getFailedNodeId());
        payload.setFailedNodeName(event.getFailedNodeName());
        payload.setTargetNodeId(event.getTargetNodeId());
        payload.setTargetNodeName(event.getTargetNodeName());
        payload.setPayloadFormat(event.getPayloadFormat());
        payload.setPayloadData(event.getPayloadData());
        payload.setPayloadHash(event.getPayloadHash());
        payload.setPayloadSize(event.getPayloadSize());
        payload.setPayloadComplete(event.getPayloadComplete());
        payload.setPayloadPreview(event.getPayloadPreview());
        payload.setPayloadPreviewTruncated(event.getPayloadPreviewTruncated());
        return payload;
    }

    public DqlEventSummaryVo summary(DqlEventQueryVo query, UserDetail user) {
        DqlEventQueryVo summaryQuery = summaryQuery(query);
        DqlEventQueryResolution resolved = resolveQuery(summaryQuery, user);
        DqlEventSummaryVo summary = new DqlEventSummaryVo();
        if (resolved.visibleTaskIds() == null) {
            summary.setTotal(eventRepository.count(resolved.query()));
            summary.setPending(eventRepository.countByStatus(resolved.query(), DqlEventStatusEnum.PENDING));
            summary.setReprocessing(eventRepository.countByStatus(resolved.query(), DqlEventStatusEnum.REPROCESSING));
            summary.setRecovered(eventRepository.countByStatus(resolved.query(), DqlEventStatusEnum.RECOVERED));
            summary.setRecoveryFailed(eventRepository.countByStatus(resolved.query(), DqlEventStatusEnum.RECOVERY_FAILED));
            summary.setNotReprocessable(eventRepository.countByStatus(resolved.query(), DqlEventStatusEnum.NOT_REPROCESSABLE));
        } else {
            summary.setTotal(eventRepository.count(resolved.query(), resolved.visibleTaskIds()));
            summary.setPending(eventRepository.countByStatus(resolved.query(), DqlEventStatusEnum.PENDING, resolved.visibleTaskIds()));
            summary.setReprocessing(eventRepository.countByStatus(resolved.query(), DqlEventStatusEnum.REPROCESSING, resolved.visibleTaskIds()));
            summary.setRecovered(eventRepository.countByStatus(resolved.query(), DqlEventStatusEnum.RECOVERED, resolved.visibleTaskIds()));
            summary.setRecoveryFailed(eventRepository.countByStatus(resolved.query(), DqlEventStatusEnum.RECOVERY_FAILED, resolved.visibleTaskIds()));
            summary.setNotReprocessable(eventRepository.countByStatus(resolved.query(), DqlEventStatusEnum.NOT_REPROCESSABLE, resolved.visibleTaskIds()));
        }
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

    /**
     * Receives the safe observability signal for an Engine Storm Guard decision.
     * This path only creates the task alarm; it never persists an exception event.
     */
    public void reportStormGuard(String taskId, DqlStormGuardReportVo report) {
        if (StringUtils.isBlank(taskId)) {
            throw new BizException("IllegalArgument", "taskId");
        }
        if (report == null) {
            throw new BizException("IllegalArgument", "vo");
        }
        report.setTaskId(taskId);
        alarmService.notifyStormGuard(report);
    }

    private Collection<String> resolveQueryTaskIds(DqlEventQueryVo query, UserDetail user) {
        if (permissionService == null) {
            return null;
        }
        Collection<String> taskIds = permissionService.resolveVisibleTaskIds(query, user);
        return taskIds == null ? List.of() : taskIds;
    }

    private DqlEventQueryResolution resolveQuery(DqlEventQueryVo query, UserDetail user) {
        Collection<String> visibleTaskIds = resolveQueryTaskIds(query, user);
        if (taskService == null || query == null || StringUtils.isBlank(query.getTaskName())) {
            return new DqlEventQueryResolution(query, visibleTaskIds);
        }

        Collection<String> matchedTaskIds = resolveTaskIdsByCurrentName(query.getTaskName(), visibleTaskIds, user);
        DqlEventQueryVo effectiveQuery = new DqlEventQueryVo();
        BeanUtils.copyProperties(query, effectiveQuery);
        effectiveQuery.setTaskName(null);
        return new DqlEventQueryResolution(effectiveQuery, matchedTaskIds);
    }

    private Collection<String> resolveTaskIdsByCurrentName(String taskName,
                                                            Collection<String> visibleTaskIds,
                                                            UserDetail user) {
        List<ObjectId> taskObjectIds = visibleTaskIds == null
                ? List.of()
                : visibleTaskIds.stream()
                        .filter(ObjectId::isValid)
                        .map(ObjectId::new)
                        .toList();
        Criteria criteria = Criteria.where("name").regex(Pattern.quote(taskName), "i")
                .and("is_deleted").ne(true);
        if (visibleTaskIds != null) {
            if (taskObjectIds.isEmpty()) {
                return List.of();
            }
            criteria.and("_id").in(taskObjectIds);
        }
        Query taskQuery = Query.query(criteria);
        taskQuery.fields().include("_id").include("name");
        List<TaskEntity> tasks = taskService.findAll(taskQuery, user);
        if (tasks == null || tasks.isEmpty()) {
            return List.of();
        }
        return tasks.stream()
                .filter(task -> task.getId() != null)
                .map(task -> task.getId().toHexString())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private Map<String, String> resolveCurrentTaskNames(List<DqlEventDto> events, UserDetail user) {
        if (taskService == null || events == null || events.isEmpty()) {
            return Map.of();
        }
        List<ObjectId> taskObjectIds = events.stream()
                .map(DqlEventDto::getTaskId)
                .filter(StringUtils::isNotBlank)
                .filter(ObjectId::isValid)
                .map(ObjectId::new)
                .distinct()
                .toList();
        if (taskObjectIds.isEmpty()) {
            return Map.of();
        }
        Query taskQuery = Query.query(Criteria.where("_id").in(taskObjectIds)
                .and("is_deleted").ne(true));
        taskQuery.fields().include("_id").include("name");
        List<TaskEntity> tasks = taskService.findAll(taskQuery, user);
        if (tasks == null || tasks.isEmpty()) {
            return Map.of();
        }
        return tasks.stream()
                .filter(task -> task.getId() != null)
                .collect(Collectors.toMap(
                        task -> task.getId().toHexString(),
                        TaskEntity::getName,
                        (left, right) -> left,
                        LinkedHashMap::new
                ));
    }

    private String currentTaskName(DqlEventDto event, Map<String, String> currentTaskNames) {
        if (event == null) {
            return null;
        }
        return taskService == null
                ? event.getTaskName()
                : currentTaskNames.get(event.getTaskId());
    }

    private String resolveCurrentTaskName(DqlEventDto event) {
        if (event == null) {
            return null;
        }
        if (taskService == null) {
            return event.getTaskName();
        }
        if (StringUtils.isBlank(event.getTaskId()) || !ObjectId.isValid(event.getTaskId())) {
            return null;
        }
        TaskDto task = taskService.findByTaskId(new ObjectId(event.getTaskId()), "name");
        return task == null ? null : task.getName();
    }

    private record DqlEventQueryResolution(DqlEventQueryVo query, Collection<String> visibleTaskIds) {
    }

    private void checkMenuPermission(UserDetail user) {
        if (permissionService == null) {
            return;
        }
        permissionService.checkMenuVisible(user);
    }

    private void checkEventTaskPermission(DqlEventDto event, UserDetail user) {
        if (permissionService == null) {
            return;
        }
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
        if (Boolean.FALSE.equals(dto.getPayloadComplete())) {
            dto.setNotReprocessableReason("DqlRecovery.Preview.PayloadIncomplete");
        }
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
