package com.tapdata.tm.dql.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.DqlRecoveryCallbackResultEnum;
import com.tapdata.tm.dql.DqlRecoveryAttemptResultEnum;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto;
import com.tapdata.tm.dql.entity.DqlEventEntity;
import com.tapdata.tm.dql.vo.DqlRecordSuccessReportVo;
import com.tapdata.tm.task.entity.TaskEntity;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.index.PartialIndexFilter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Repository
public class DqlEventRepository {
    private final MongoTemplate mongoTemplate;
    private final Class<DqlEventEntity> entityClass;
    private final String collectionName;

    public DqlEventRepository(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
        this.entityClass = DqlEventEntity.class;
        this.collectionName = Optional.of(entityClass)
                .map(clz -> clz.getAnnotation(org.springframework.data.mongodb.core.mapping.Document.class))
                .map(org.springframework.data.mongodb.core.mapping.Document::value)
                .orElseThrow(() -> new IllegalArgumentException("Class " + entityClass.getSimpleName() + " is not a document"));
        init();
    }

    protected void init() {
        if (!mongoTemplate.collectionExists(collectionName)) {
            mongoTemplate.createCollection(collectionName);
        }
        ensureIndexes();
    }

    private void ensureIndexes() {
        IndexOperations indexOps = mongoTemplate.indexOps(collectionName);
        indexOps.createIndex(new Index()
                .on(DqlEventDto.FIELD_TASK_ID, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_EVENT_TIME, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_CAPTURE_SEQ, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_EVENT_ID, Sort.Direction.ASC)
                .named("idx_task_event_time"));
        indexOps.createIndex(new Index()
                .on(DqlEventDto.FIELD_TASK_ID, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_STATUS, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_FAILED_AT, Sort.Direction.DESC)
                .named("idx_task_status_failed_at"));
        indexOps.createIndex(new Index()
                .on(DqlEventDto.FIELD_TASK_ID, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_TASK_VERSION, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_STATUS, Sort.Direction.ASC)
                .named("idx_task_version_status"));
        indexOps.createIndex(new Index()
                .on(DqlEventDto.FIELD_STATUS, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_FAILED_AT, Sort.Direction.DESC)
                .named("idx_status_failed_at"));
        indexOps.createIndex(new Index()
                .on(DqlEventDto.FIELD_TASK_ID, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_SOURCE_TABLE, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_FAILED_AT, Sort.Direction.DESC)
                .named("idx_task_source_table_failed_at"));
        indexOps.createIndex(new Index()
                .on(DqlEventDto.FIELD_EVENT_ID, Sort.Direction.ASC)
                .unique()
                .named("uk_event_id"));
        indexOps.createIndex(new Index()
                .on(DqlEventDto.FIELD_TASK_ID, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_TASK_RECORD_ID, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_TABLE_ID, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_RECORD_IDENTITY, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_EVENT_TIME, Sort.Direction.DESC)
                .on(DqlEventDto.FIELD_CAPTURE_SEQ, Sort.Direction.DESC)
                .named("idx_task_record_identity_event_time"));
        indexOps.createIndex(new Index()
                .on(DqlEventDto.FIELD_TASK_ID, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_TASK_RECORD_ID, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_TABLE_ID, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_EVENT_IDENTITY, Sort.Direction.ASC)
                .on(DqlEventDto.FIELD_FAILED_NODE_ID, Sort.Direction.ASC)
                .partial(PartialIndexFilter.of(new org.bson.Document(
                        DqlEventDto.FIELD_EVENT_IDENTITY,
                        new org.bson.Document("$type", "string").append("$gt", "")
                )))
                .unique()
                .named("uk_task_event_identity"));
    }

    public Long nextCaptureSeq(String taskId) {
        if (!ObjectId.isValid(taskId)) {
            throw new BizException("IllegalArgument", "taskId");
        }
        Query query = Query.query(Criteria.where("_id").is(new ObjectId(taskId)));
        Update update = new Update().inc("attrs.dqlEventSeq", 1);
        TaskEntity task = mongoTemplate.findAndModify(
                query,
                update,
                FindAndModifyOptions.options().returnNew(true),
                TaskEntity.class
        );
        if (task == null) {
            throw new BizException("Task.NotFound", taskId);
        }
        Object seq = Optional.ofNullable(task.getAttrs()).map(attrs -> attrs.get("dqlEventSeq")).orElse(1L);
        if (seq instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(seq));
    }

    public DqlEventDto findDuplicate(String taskId, com.tapdata.tm.dql.vo.DqlEventReportVo report) {
        if (report == null || StringUtils.isBlank(report.getEventIdentity())) {
            return null;
        }
        Criteria criteria = Criteria.where(DqlEventDto.FIELD_TASK_ID).is(taskId)
                .and(DqlEventDto.FIELD_TASK_RECORD_ID).is(report.getTaskRecordId())
                .and(DqlEventDto.FIELD_TABLE_ID).is(report.getTableId())
                .and(DqlEventDto.FIELD_EVENT_IDENTITY).is(report.getEventIdentity())
                .and(DqlEventDto.FIELD_FAILED_NODE_ID).is(report.getFailedNodeId());
        return convert(mongoTemplate.findOne(Query.query(criteria), entityClass));
    }

    public DqlEventDto upsert(DqlEventDto dto) {
        Assert.notNull(dto, "DTO must not be null!");
        Assert.notNull(dto.getTaskId(), "taskId must not be null!");
        Assert.notNull(dto.getEventId(), "eventId must not be null!");

        Query query = Query.query(uniqueCriteria(dto));
        Date now = new Date();
        Date created = Optional.ofNullable(dto.getCreated()).orElse(now);
        Date ttlAt = created;
        Update update = new Update();
        setOnInsert(update, DqlEventDto.FIELD_EVENT_ID, dto.getEventId());
        setOnInsert(update, DqlEventDto.FIELD_TASK_ID, dto.getTaskId());
        setOnInsert(update, DqlEventDto.FIELD_TASK_RECORD_ID, dto.getTaskRecordId());
        setOnInsert(update, DqlEventDto.FIELD_TASK_NAME, dto.getTaskName());
        setOnInsert(update, DqlEventDto.FIELD_TASK_VERSION, dto.getTaskVersion());
        setOnInsert(update, DqlEventDto.FIELD_AGENT_ID, dto.getAgentId());
        setOnInsert(update, DqlEventDto.FIELD_SOURCE_NODE_ID, dto.getSourceNodeId());
        setOnInsert(update, DqlEventDto.FIELD_SOURCE_NODE_NAME, dto.getSourceNodeName());
        setOnInsert(update, DqlEventDto.FIELD_TARGET_NODE_ID, dto.getTargetNodeId());
        setOnInsert(update, DqlEventDto.FIELD_TARGET_NODE_NAME, dto.getTargetNodeName());
        setOnInsert(update, DqlEventDto.FIELD_FAILED_NODE_ID, dto.getFailedNodeId());
        setOnInsert(update, DqlEventDto.FIELD_FAILED_NODE_NAME, dto.getFailedNodeName());
        setOnInsert(update, DqlEventDto.FIELD_FAILED_STAGE, dto.getFailedStage());
        setOnInsert(update, DqlEventDto.FIELD_SOURCE_TABLE, dto.getSourceTable());
        setOnInsert(update, DqlEventDto.FIELD_TARGET_TABLE, dto.getTargetTable());
        setOnInsert(update, DqlEventDto.FIELD_TABLE_ID, dto.getTableId());
        setOnInsert(update, DqlEventDto.FIELD_DML_TYPE, dto.getDmlType());
        setOnInsert(update, DqlEventDto.FIELD_EVENT_TIME, dto.getEventTime());
        setOnInsert(update, DqlEventDto.FIELD_CAPTURE_SEQ, dto.getCaptureSeq());
        setOnInsert(update, DqlEventDto.FIELD_FAILED_AT, dto.getFailedAt());
        setOnInsert(update, DqlEventDto.FIELD_EVENT_KEY, dto.getEventKey());
        setOnInsert(update, DqlEventDto.FIELD_EVENT_KEY_MISSING, dto.getEventKeyMissing());
        setOnInsert(update, DqlEventDto.FIELD_EVENT_IDENTITY, dto.getEventIdentity());
        setOnInsert(update, DqlEventDto.FIELD_RECORD_IDENTITY, dto.getRecordIdentity());
        setOnInsert(update, DqlEventDto.FIELD_RECORD_IDENTITY_TYPE, dto.getRecordIdentityType());
        setOnInsert(update, DqlEventDto.FIELD_RECORD_IDENTITY_FIELDS, dto.getRecordIdentityFields());
        setOnInsert(update, DqlEventDto.FIELD_PAYLOAD_FORMAT, dto.getPayloadFormat());
        setOnInsert(update, DqlEventDto.FIELD_PAYLOAD_DATA, dto.getPayloadData());
        setOnInsert(update, DqlEventDto.FIELD_PAYLOAD_HASH, dto.getPayloadHash());
        setOnInsert(update, DqlEventDto.FIELD_PAYLOAD_SIZE, dto.getPayloadSize());
        setOnInsert(update, DqlEventDto.FIELD_PAYLOAD_COMPLETE, dto.getPayloadComplete());
        setOnInsert(update, DqlEventDto.FIELD_PAYLOAD_PREVIEW, dto.getPayloadPreview());
        setOnInsert(update, DqlEventDto.FIELD_PAYLOAD_PREVIEW_TRUNCATED, dto.getPayloadPreviewTruncated());
        setOnInsert(update, DqlEventDto.FIELD_ERROR_TYPE, dto.getErrorType());
        setOnInsert(update, DqlEventDto.FIELD_ERROR_CODE, dto.getErrorCode());
        setOnInsert(update, DqlEventDto.FIELD_EXCEPTION_SCOPE, dto.getExceptionScope());
        setOnInsert(update, DqlEventDto.FIELD_ROUTE_DECISION, dto.getRouteDecision());
        setOnInsert(update, DqlEventDto.FIELD_CLASSIFICATION_REASON, dto.getClassificationReason());
        setOnInsert(update, DqlEventDto.FIELD_CLASSIFICATION_CONFIDENCE, dto.getClassificationConfidence());
        setOnInsert(update, DqlEventDto.FIELD_ERROR_DETAILS, dto.getErrorDetails());
        setOnInsert(update, DqlEventDto.FIELD_ERROR_DETAILS_TRUNCATED, dto.getErrorDetailsTruncated());
        setOnInsert(update, DqlEventDto.FIELD_RAW_ERROR_REF, dto.getRawErrorRef());
        setOnInsert(update, DqlEventDto.FIELD_STATUS, dto.getStatus());
        setOnInsert(update, DqlEventDto.FIELD_RECOVERY_COUNT, Optional.ofNullable(dto.getRecoveryCount()).orElse(0));
        setOnInsert(update, DqlEventDto.FIELD_OVERWRITE_RISK, dto.getOverwriteRisk());
        setOnInsert(update, DqlEventDto.FIELD_OVERWRITE_RISK_MESSAGE, dto.getOverwriteRiskMessage());
        setOnInsert(update, DqlEventDto.FIELD_LATER_SUCCESS_AT, dto.getLaterSuccessAt());
        setOnInsert(update, DqlEventDto.FIELD_LATER_SUCCESS_EVENT_TIME, dto.getLaterSuccessEventTime());
        setOnInsert(update, DqlEventDto.FIELD_LATER_SUCCESS_CAPTURE_SEQ, dto.getLaterSuccessCaptureSeq());
        setOnInsert(update, DqlEventDto.FIELD_LATER_SUCCESS_DML_TYPE, dto.getLaterSuccessDmlType());
        setOnInsert(update, DqlEventDto.FIELD_UPDATED, now);
        update.setOnInsert(DqlEventDto.FIELD_CREATED, created);
        update.setOnInsert(DqlEventDto.FIELD_TTL_AT, ttlAt);

        DqlEventEntity entity = mongoTemplate.findAndModify(
                query,
                update,
                FindAndModifyOptions.options().returnNew(true).upsert(true),
                entityClass
        );
        return convert(entity);
    }

    public DqlEventDto markLaterSuccess(String taskId, DqlRecordSuccessReportVo report, String riskMessage) {
        if (StringUtils.isBlank(taskId) || report == null || StringUtils.isBlank(report.getRecordIdentity())) {
            return null;
        }
        Date successAt = new Date(Optional.ofNullable(report.getSuccessAt()).orElseGet(() -> Optional.ofNullable(report.getEventTime()).orElse(System.currentTimeMillis())));
        Date successEventTime = report.getEventTime() == null ? null : new Date(report.getEventTime());
        Criteria criteria = Criteria.where(DqlEventDto.FIELD_TASK_ID).is(taskId)
                .and(DqlEventDto.FIELD_RECORD_IDENTITY).is(report.getRecordIdentity())
                .and(DqlEventDto.FIELD_STATUS).in(
                        DqlEventStatusEnum.PENDING.name(),
                        DqlEventStatusEnum.REPROCESSING.name(),
                        DqlEventStatusEnum.RECOVERY_FAILED.name()
                );
        if (StringUtils.isNotBlank(report.getTaskRecordId())) {
            criteria.and(DqlEventDto.FIELD_TASK_RECORD_ID).is(report.getTaskRecordId());
        }
        if (StringUtils.isNotBlank(report.getTableId())) {
            criteria.and(DqlEventDto.FIELD_TABLE_ID).is(report.getTableId());
        }
        if (successEventTime != null) {
            criteria.and(DqlEventDto.FIELD_EVENT_TIME).lte(successEventTime);
        }
        Query query = Query.query(criteria).with(Sort.by(
                Sort.Order.desc(DqlEventDto.FIELD_EVENT_TIME),
                Sort.Order.desc(DqlEventDto.FIELD_CAPTURE_SEQ),
                Sort.Order.desc(DqlEventDto.FIELD_FAILED_AT)
        ));
        Update update = new Update()
                .set(DqlEventDto.FIELD_OVERWRITE_RISK, true)
                .set(DqlEventDto.FIELD_OVERWRITE_RISK_MESSAGE, riskMessage)
                .set(DqlEventDto.FIELD_LATER_SUCCESS_AT, successAt)
                .set(DqlEventDto.FIELD_LATER_SUCCESS_CAPTURE_SEQ, report.getCaptureSeq())
                .set(DqlEventDto.FIELD_LATER_SUCCESS_DML_TYPE, report.getDmlType())
                .set(DqlEventDto.FIELD_UPDATED, new Date());
        if (successEventTime != null) {
            update.set(DqlEventDto.FIELD_LATER_SUCCESS_EVENT_TIME, successEventTime);
        }
        DqlEventEntity entity = mongoTemplate.findAndModify(
                query,
                update,
                FindAndModifyOptions.options().returnNew(true),
                entityClass
        );
        return convert(entity);
    }

    public DqlEventDto findByEventId(String eventId) {
        if (StringUtils.isBlank(eventId)) {
            return null;
        }
        return convert(mongoTemplate.findOne(Query.query(Criteria.where(DqlEventDto.FIELD_EVENT_ID).is(eventId)), entityClass));
    }

    public List<DqlEventDto> findByEventIds(List<String> eventIds) {
        if (eventIds == null || eventIds.isEmpty()) {
            return List.of();
        }
        Query query = Query.query(Criteria.where(DqlEventDto.FIELD_EVENT_ID).in(eventIds));
        return mongoTemplate.find(query, entityClass).stream().map(this::convert).collect(Collectors.toList());
    }

    public Page<DqlEventDto> page(com.tapdata.tm.dql.vo.DqlEventQueryVo queryVo) {
        return page(queryVo, null);
    }

    public Page<DqlEventDto> page(com.tapdata.tm.dql.vo.DqlEventQueryVo queryVo, Collection<String> visibleTaskIds) {
        Query query = new Query(buildCriteria(queryVo, visibleTaskIds));
        long count = mongoTemplate.count(query, entityClass);
        if (count == 0) {
            return Page.empty();
        }
        long skip = Math.max(0, Optional.ofNullable(queryVo).map(com.tapdata.tm.dql.vo.DqlEventQueryVo::getSkip).orElse(0L));
        int requestedLimit = Optional.ofNullable(queryVo).map(com.tapdata.tm.dql.vo.DqlEventQueryVo::getLimit).orElse(0);
        int limit = requestedLimit > 0 ? requestedLimit : 20;
        query.skip(skip).limit(limit).with(parseSort(Optional.ofNullable(queryVo).map(com.tapdata.tm.dql.vo.DqlEventQueryVo::getOrder).orElse(null)));
        List<DqlEventDto> items = mongoTemplate.find(query, entityClass).stream().map(this::convert).collect(Collectors.toList());
        return Page.page(items, count);
    }

    public long count(com.tapdata.tm.dql.vo.DqlEventQueryVo queryVo) {
        return count(queryVo, null);
    }

    public long count(com.tapdata.tm.dql.vo.DqlEventQueryVo queryVo, Collection<String> visibleTaskIds) {
        return mongoTemplate.count(new Query(buildCriteria(queryVo, visibleTaskIds)), entityClass);
    }

    public long countPendingByTaskId(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return 0;
        }
        Query query = Query.query(Criteria.where(DqlEventDto.FIELD_TASK_ID).is(taskId)
                .and(DqlEventDto.FIELD_STATUS).in(DqlEventStatusEnum.PENDING.name(),
                        DqlEventStatusEnum.RECOVERY_FAILED.name()));
        return mongoTemplate.count(query, entityClass);
    }

    public Map<String, Long> countByTaskIdAndVersion(Map<String, Long> taskVersions) {
        if (taskVersions == null || taskVersions.isEmpty()) {
            return Map.of();
        }
        List<Criteria> taskVersionCriteria = taskVersions.entrySet().stream()
                .filter(entry -> StringUtils.isNotBlank(entry.getKey()) && entry.getValue() != null)
                .map(entry -> Criteria.where(DqlEventDto.FIELD_TASK_ID).is(entry.getKey())
                        .and(DqlEventDto.FIELD_TASK_VERSION).is(entry.getValue()))
                .toList();
        if (taskVersionCriteria.isEmpty()) {
            return Map.of();
        }

        Criteria matchCriteria = new Criteria().andOperator(
                new Criteria().orOperator(taskVersionCriteria.toArray(new Criteria[0])),
                Criteria.where(DqlEventDto.FIELD_STATUS).in(
                        DqlEventStatusEnum.PENDING.name(),
                        DqlEventStatusEnum.REPROCESSING.name(),
                        DqlEventStatusEnum.RECOVERY_FAILED.name())
        );
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(matchCriteria),
                Aggregation.group(DqlEventDto.FIELD_TASK_ID).count().as("count")
        );
        AggregationResults<Document> results = mongoTemplate.aggregate(aggregation, collectionName, Document.class);
        if (results == null || results.getMappedResults() == null) {
            return Map.of();
        }
        return results.getMappedResults().stream()
                .filter(result -> result.getString("_id") != null)
                .collect(Collectors.toMap(
                        result -> result.getString("_id"),
                        result -> {
                            Number count = result.get("count", Number.class);
                            return count == null ? 0L : count.longValue();
                        }
                ));
    }

    public long countByStatus(com.tapdata.tm.dql.vo.DqlEventQueryVo queryVo, DqlEventStatusEnum status) {
        return countByStatus(queryVo, status, null);
    }

    public long countByStatus(com.tapdata.tm.dql.vo.DqlEventQueryVo queryVo,
                              DqlEventStatusEnum status,
                              Collection<String> visibleTaskIds) {
        com.tapdata.tm.dql.vo.DqlEventQueryVo scoped = new com.tapdata.tm.dql.vo.DqlEventQueryVo();
        if (queryVo != null) {
            BeanUtils.copyProperties(queryVo, scoped);
        }
        scoped.setStatus(status.name());
        return count(scoped, visibleTaskIds);
    }

    public long lockEvents(List<String> eventIds, String batchId) {
        if (eventIds == null || eventIds.isEmpty() || StringUtils.isBlank(batchId)) {
            return 0;
        }
        Criteria criteria = Criteria.where(DqlEventDto.FIELD_EVENT_ID).in(eventIds)
                .and(DqlEventDto.FIELD_STATUS).in(DqlEventStatusEnum.PENDING.name(), DqlEventStatusEnum.RECOVERY_FAILED.name())
                .and(DqlEventDto.FIELD_CURRENT_BATCH_ID).is(null);
        Query query = Query.query(criteria);
        Date now = new Date();
        Update update = new Update()
                .set(DqlEventDto.FIELD_STATUS, DqlEventStatusEnum.REPROCESSING.name())
                .set(DqlEventDto.FIELD_CURRENT_BATCH_ID, batchId)
                .set(DqlEventDto.FIELD_UPDATED, now)
                .set(DqlEventDto.FIELD_TTL_AT, now);
        UpdateResult result = mongoTemplate.updateMulti(query, update, entityClass);
        return result.getModifiedCount();
    }

    /**
     * Finalizes the running recovery attempt for each event still owned by the batch.
     * This is used when the batch fails without an event-level terminal callback.
     */
    public long finalizeRunningAttempts(String batchId,
                                        List<String> eventIds,
                                        DqlRecoveryAttemptResultEnum result,
                                        String message,
                                        Date now) {
        if (StringUtils.isBlank(batchId) || result == null || result == DqlRecoveryAttemptResultEnum.RUNNING
                || now == null) {
            return 0;
        }
        Criteria criteria = Criteria.where(DqlEventDto.FIELD_CURRENT_BATCH_ID).is(batchId)
                .and(DqlEventDto.FIELD_STATUS).is(DqlEventStatusEnum.REPROCESSING.name())
                .and(DqlEventDto.FIELD_RECOVERY_ATTEMPTS)
                .elemMatch(Criteria.where(DqlRecoveryAttemptDto.FIELD_BATCH_ID).is(batchId)
                        .and(DqlRecoveryAttemptDto.FIELD_RESULT).is(DqlRecoveryAttemptResultEnum.RUNNING.name()));
        if (eventIds != null && !eventIds.isEmpty()) {
            criteria.and(DqlEventDto.FIELD_EVENT_ID).in(eventIds);
        }
        Query query = Query.query(criteria);
        Update update = recoveryFailureUpdate(result, now);
        String attemptPath = DqlEventDto.FIELD_RECOVERY_ATTEMPTS + ".$.";
        update.set(attemptPath + DqlRecoveryAttemptDto.FIELD_RESULT, result.name())
                .set(attemptPath + DqlRecoveryAttemptDto.FIELD_FINISHED_AT, now);
        if (message != null) {
            update.set(attemptPath + DqlRecoveryAttemptDto.FIELD_MESSAGE, message);
        }
        return modifiedCount(mongoTemplate.updateMulti(query, update, entityClass));
    }

    /**
     * Finalizes events which were claimed by a recovery batch but never
     * emitted EVENT_STARTED. This closes the batch lifecycle even when the
     * Engine fails while preparing the recovery runtime or before it can
     * process the first event.
     *
     * <p>The batch id is part of the attempt identity and the query excludes
     * events that already contain an attempt for this batch. Consequently a
     * repeated BATCH_FAILED callback is a no-op.</p>
     */
    public long finalizeUnstartedAttempts(String batchId,
                                          List<String> eventIds,
                                          DqlRecoveryAttemptDto attempt,
                                          Date now) {
        if (StringUtils.isBlank(batchId) || attempt == null
                || StringUtils.isBlank(attempt.getAttemptId())
                || !StringUtils.equals(batchId, attempt.getBatchId())
                || !DqlRecoveryAttemptResultEnum.FAILED.name().equals(attempt.getResult())
                || now == null) {
            return 0;
        }
        Criteria criteria = Criteria.where(DqlEventDto.FIELD_CURRENT_BATCH_ID).is(batchId)
                .and(DqlEventDto.FIELD_STATUS).is(DqlEventStatusEnum.REPROCESSING.name())
                .and(DqlEventDto.FIELD_RECOVERY_ATTEMPTS)
                .not()
                .elemMatch(Criteria.where(DqlRecoveryAttemptDto.FIELD_BATCH_ID).is(batchId));
        if (eventIds != null && !eventIds.isEmpty()) {
            criteria.and(DqlEventDto.FIELD_EVENT_ID).in(eventIds);
        }
        Query query = Query.query(criteria);
        Update update = recoveryFailureUpdate(DqlRecoveryAttemptResultEnum.FAILED, now)
                .push(DqlEventDto.FIELD_RECOVERY_ATTEMPTS, attempt);
        if (attempt.getOperatorId() != null) {
            update.set(DqlEventDto.FIELD_LAST_RECOVERY_USER_ID, attempt.getOperatorId());
        }
        if (attempt.getOperatorName() != null) {
            update.set(DqlEventDto.FIELD_LAST_RECOVERY_USER_NAME, attempt.getOperatorName());
        }
        return modifiedCount(mongoTemplate.updateMulti(query, update, entityClass));
    }

    /**
     * Moves all still-running events for a batch to recovery failure when the batch times out.
     * Existing RUNNING attempts are updated in place. Events that never emitted EVENT_STARTED
     * receive one stable synthetic timeout attempt as a compatibility fallback.
     */
    public long timeoutEvents(String batchId, List<String> eventIds, Date now) {
        if (StringUtils.isBlank(batchId) || now == null) {
            return 0;
        }
        long finalized = finalizeRunningAttempts(batchId, eventIds,
                DqlRecoveryAttemptResultEnum.TIMEOUT, "Recovery batch timed out", now);
        if (eventIds != null && !eventIds.isEmpty() && finalized >= eventIds.size()) {
            return finalized;
        }
        return finalized + appendTimeoutAttempts(batchId, eventIds, now);
    }

    private long appendTimeoutAttempts(String batchId, List<String> eventIds, Date now) {
        String attemptId = "TIMEOUT-" + batchId;
        Criteria criteria = Criteria.where(DqlEventDto.FIELD_CURRENT_BATCH_ID).is(batchId)
                .and(DqlEventDto.FIELD_STATUS).is(DqlEventStatusEnum.REPROCESSING.name())
                .and(DqlEventDto.FIELD_RECOVERY_ATTEMPTS)
                .not()
                .elemMatch(attemptIdentityCriteria(batchId, attemptId));
        if (eventIds != null && !eventIds.isEmpty()) {
            criteria.and(DqlEventDto.FIELD_EVENT_ID).in(eventIds);
        }
        Query query = Query.query(criteria);
        DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
        attempt.setAttemptId(attemptId);
        attempt.setBatchId(batchId);
        attempt.setStartedAt(now);
        attempt.setFinishedAt(now);
        attempt.setResult(DqlRecoveryAttemptResultEnum.TIMEOUT.name());
        attempt.setMessage("Recovery batch timed out");
        Update update = recoveryFailureUpdate(DqlRecoveryAttemptResultEnum.TIMEOUT, now);
        update.push(DqlEventDto.FIELD_RECOVERY_ATTEMPTS, attempt);
        return modifiedCount(mongoTemplate.updateMulti(query, update, entityClass));
    }

    public long countReprocessingByBatchId(String batchId) {
        if (StringUtils.isBlank(batchId)) {
            return 0;
        }
        Query query = Query.query(Criteria.where(DqlEventDto.FIELD_CURRENT_BATCH_ID).is(batchId)
                .and(DqlEventDto.FIELD_STATUS).is(DqlEventStatusEnum.REPROCESSING.name()));
        return mongoTemplate.count(query, entityClass);
    }

    public boolean completeEvent(String eventId, String batchId, DqlRecoveryAttemptDto attempt) {
        Query query = batchEventQuery(eventId, batchId);
        Date now = new Date();
        Update update = new Update()
                .set(DqlEventDto.FIELD_STATUS, DqlEventStatusEnum.RECOVERED.name())
                .set(DqlEventDto.FIELD_CURRENT_BATCH_ID, null)
                .set(DqlEventDto.FIELD_LAST_RECOVERY_TIME, attempt.getFinishedAt())
                .set(DqlEventDto.FIELD_LAST_RECOVERY_USER_ID, attempt.getOperatorId())
                .set(DqlEventDto.FIELD_LAST_RECOVERY_USER_NAME, attempt.getOperatorName())
                .set(DqlEventDto.FIELD_LAST_RECOVERY_RESULT, DqlRecoveryAttemptResultEnum.SUCCESS.name())
                .inc(DqlEventDto.FIELD_RECOVERY_COUNT, 1)
                .set(DqlEventDto.FIELD_UPDATED, now)
                .set(DqlEventDto.FIELD_TTL_AT, now);
        update.push(DqlEventDto.FIELD_RECOVERY_ATTEMPTS, attempt);
        return mongoTemplate.updateFirst(query, update, entityClass).getModifiedCount() > 0;
    }

    /**
     * Completes an event while treating a previously applied attempt as a no-op.
     */
    public DqlRecoveryCallbackResultEnum completeEventIdempotent(String eventId,
                                                                  String batchId,
                                                                  DqlRecoveryAttemptDto attempt) {
        return updateTerminalEventIdempotent(eventId, batchId, attempt,
                DqlRecoveryAttemptResultEnum.SUCCESS, DqlEventStatusEnum.RECOVERED);
    }

    public boolean failEvent(String eventId, String batchId, DqlRecoveryAttemptDto attempt) {
        Query query = batchEventQuery(eventId, batchId);
        Date now = new Date();
        Update update = new Update()
                .set(DqlEventDto.FIELD_STATUS, DqlEventStatusEnum.RECOVERY_FAILED.name())
                .set(DqlEventDto.FIELD_CURRENT_BATCH_ID, null)
                .set(DqlEventDto.FIELD_LAST_RECOVERY_TIME, attempt.getFinishedAt())
                .set(DqlEventDto.FIELD_LAST_RECOVERY_USER_ID, attempt.getOperatorId())
                .set(DqlEventDto.FIELD_LAST_RECOVERY_USER_NAME, attempt.getOperatorName())
                .set(DqlEventDto.FIELD_LAST_RECOVERY_RESULT, attempt.getResult())
                .inc(DqlEventDto.FIELD_RECOVERY_COUNT, 1)
                .set(DqlEventDto.FIELD_UPDATED, now)
                .set(DqlEventDto.FIELD_TTL_AT, now);
        update.push(DqlEventDto.FIELD_RECOVERY_ATTEMPTS, attempt);
        return mongoTemplate.updateFirst(query, update, entityClass).getModifiedCount() > 0;
    }

    /**
     * Fails an event while treating a previously applied attempt as a no-op.
     */
    public DqlRecoveryCallbackResultEnum failEventIdempotent(String eventId,
                                                              String batchId,
                                                              DqlRecoveryAttemptDto attempt) {
        DqlRecoveryAttemptResultEnum result = DqlRecoveryAttemptResultEnum.parse(attempt == null ? null : attempt.getResult());
        if (result == null || result == DqlRecoveryAttemptResultEnum.RUNNING
                || result == DqlRecoveryAttemptResultEnum.SUCCESS) {
            return DqlRecoveryCallbackResultEnum.NOT_IN_BATCH;
        }
        return updateTerminalEventIdempotent(eventId, batchId, attempt, result,
                DqlEventStatusEnum.RECOVERY_FAILED);
    }

    /**
     * Records that the recovery engine has started processing an event.
     * Ownership remains guarded by the current batch lock and the event stays
     * in REPROCESSING until a terminal result is reported.
     */
    public boolean startEvent(String eventId, String batchId, DqlRecoveryAttemptDto attempt) {
        Query query = batchEventQuery(eventId, batchId);
        Date now = new Date();
        Update update = new Update()
                .set(DqlEventDto.FIELD_UPDATED, now)
                .set(DqlEventDto.FIELD_TTL_AT, now);
        update.push(DqlEventDto.FIELD_RECOVERY_ATTEMPTS, attempt);
        return mongoTemplate.updateFirst(query, update, entityClass).getModifiedCount() > 0;
    }

    /**
     * Records an event start once for the same batch and attempt.
     */
    public DqlRecoveryCallbackResultEnum startEventIdempotent(String eventId,
                                                               String batchId,
                                                               DqlRecoveryAttemptDto attempt) {
        if (StringUtils.isBlank(eventId) || StringUtils.isBlank(batchId)
                || attempt == null || StringUtils.isBlank(attempt.getAttemptId())) {
            return DqlRecoveryCallbackResultEnum.NOT_IN_BATCH;
        }
        Query query = batchEventQuery(eventId, batchId);
        query.addCriteria(Criteria.where(DqlEventDto.FIELD_RECOVERY_ATTEMPTS)
                .not()
                .elemMatch(attemptIdentityCriteria(batchId, attempt.getAttemptId())));
        Date now = new Date();
        Update update = new Update()
                .set(DqlEventDto.FIELD_UPDATED, now)
                .set(DqlEventDto.FIELD_TTL_AT, now);
        update.push(DqlEventDto.FIELD_RECOVERY_ATTEMPTS, attempt);
        UpdateResult result = mongoTemplate.updateFirst(query, update, entityClass);
        if (modified(result)) {
            return DqlRecoveryCallbackResultEnum.APPLIED;
        }
        return classifyEventTransition(eventId, batchId, attempt.getAttemptId(), null, true);
    }

    private DqlRecoveryCallbackResultEnum updateTerminalEventIdempotent(String eventId,
                                                                         String batchId,
                                                                         DqlRecoveryAttemptDto attempt,
                                                                         DqlRecoveryAttemptResultEnum result,
                                                                         DqlEventStatusEnum status) {
        if (StringUtils.isBlank(eventId) || StringUtils.isBlank(batchId)
                || attempt == null || StringUtils.isBlank(attempt.getAttemptId())) {
            return DqlRecoveryCallbackResultEnum.NOT_IN_BATCH;
        }
        Query query = batchEventQuery(eventId, batchId);
        query.addCriteria(Criteria.where(DqlEventDto.FIELD_RECOVERY_ATTEMPTS)
                .elemMatch(attemptIdentityCriteria(batchId, attempt.getAttemptId())
                        .and(DqlRecoveryAttemptDto.FIELD_RESULT).nin(terminalAttemptResults())));
        Date now = new Date();
        Update update = new Update()
                .set(DqlEventDto.FIELD_STATUS, status.name())
                .set(DqlEventDto.FIELD_CURRENT_BATCH_ID, null)
                .set(DqlEventDto.FIELD_LAST_RECOVERY_TIME, attempt.getFinishedAt())
                .set(DqlEventDto.FIELD_LAST_RECOVERY_USER_ID, attempt.getOperatorId())
                .set(DqlEventDto.FIELD_LAST_RECOVERY_USER_NAME, attempt.getOperatorName())
                .set(DqlEventDto.FIELD_LAST_RECOVERY_RESULT, result.name())
                .inc(DqlEventDto.FIELD_RECOVERY_COUNT, 1)
                .set(DqlEventDto.FIELD_UPDATED, now)
                .set(DqlEventDto.FIELD_TTL_AT, now);
        setAttempt(update, DqlEventDto.FIELD_RECOVERY_ATTEMPTS + ".$.", attempt);
        UpdateResult updateResult = mongoTemplate.updateFirst(query, update, entityClass);
        if (modified(updateResult)) {
            return DqlRecoveryCallbackResultEnum.APPLIED;
        }

        // A terminal callback can arrive without a persisted EVENT_STARTED
        // callback after a transport race. Preserve that callback, but do not
        // append a second lifecycle snapshot when the attempt already exists.
        Query newAttemptQuery = batchEventQuery(eventId, batchId);
        newAttemptQuery.addCriteria(Criteria.where(DqlEventDto.FIELD_RECOVERY_ATTEMPTS)
                .not()
                .elemMatch(attemptIdentityCriteria(batchId, attempt.getAttemptId())));
        Update appendUpdate = terminalEventUpdate(attempt, result, status, now);
        appendUpdate.push(DqlEventDto.FIELD_RECOVERY_ATTEMPTS, attempt);
        UpdateResult appendResult = mongoTemplate.updateFirst(newAttemptQuery, appendUpdate, entityClass);
        if (modified(appendResult)) {
            return DqlRecoveryCallbackResultEnum.APPLIED;
        }
        return classifyEventTransition(eventId, batchId, attempt.getAttemptId(), result, false);
    }

    private Update terminalEventUpdate(DqlRecoveryAttemptDto attempt,
                                       DqlRecoveryAttemptResultEnum result,
                                       DqlEventStatusEnum status,
                                       Date now) {
        return new Update()
                .set(DqlEventDto.FIELD_STATUS, status.name())
                .set(DqlEventDto.FIELD_CURRENT_BATCH_ID, null)
                .set(DqlEventDto.FIELD_LAST_RECOVERY_TIME, attempt.getFinishedAt())
                .set(DqlEventDto.FIELD_LAST_RECOVERY_USER_ID, attempt.getOperatorId())
                .set(DqlEventDto.FIELD_LAST_RECOVERY_USER_NAME, attempt.getOperatorName())
                .set(DqlEventDto.FIELD_LAST_RECOVERY_RESULT, result.name())
                .inc(DqlEventDto.FIELD_RECOVERY_COUNT, 1)
                .set(DqlEventDto.FIELD_UPDATED, now)
                .set(DqlEventDto.FIELD_TTL_AT, now);
    }

    private Update recoveryFailureUpdate(DqlRecoveryAttemptResultEnum result, Date now) {
        return new Update()
                .set(DqlEventDto.FIELD_STATUS, DqlEventStatusEnum.RECOVERY_FAILED.name())
                .set(DqlEventDto.FIELD_CURRENT_BATCH_ID, null)
                .set(DqlEventDto.FIELD_LAST_RECOVERY_TIME, now)
                .set(DqlEventDto.FIELD_LAST_RECOVERY_RESULT, result.name())
                .inc(DqlEventDto.FIELD_RECOVERY_COUNT, 1)
                .set(DqlEventDto.FIELD_UPDATED, now)
                .set(DqlEventDto.FIELD_TTL_AT, now);
    }

    private void setAttempt(Update update,
                            String path,
                            DqlRecoveryAttemptDto attempt) {
        update.set(path + DqlRecoveryAttemptDto.FIELD_ATTEMPT_ID, attempt.getAttemptId())
                .set(path + DqlRecoveryAttemptDto.FIELD_BATCH_ID, attempt.getBatchId())
                .set(path + DqlRecoveryAttemptDto.FIELD_OPERATOR_ID, attempt.getOperatorId())
                .set(path + DqlRecoveryAttemptDto.FIELD_OPERATOR_NAME, attempt.getOperatorName())
                .set(path + DqlRecoveryAttemptDto.FIELD_TASK_VERSION, attempt.getTaskVersion())
                .set(path + DqlRecoveryAttemptDto.FIELD_STARTED_AT, attempt.getStartedAt())
                .set(path + DqlRecoveryAttemptDto.FIELD_FINISHED_AT, attempt.getFinishedAt())
                .set(path + DqlRecoveryAttemptDto.FIELD_RESULT, attempt.getResult())
                .set(path + DqlRecoveryAttemptDto.FIELD_MESSAGE, attempt.getMessage())
                .set(path + DqlRecoveryAttemptDto.FIELD_ERROR_CODE, attempt.getErrorCode())
                .set(path + DqlRecoveryAttemptDto.FIELD_ERROR_DETAILS, attempt.getErrorDetails());
    }

    private DqlRecoveryCallbackResultEnum classifyEventTransition(String eventId,
                                                                   String batchId,
                                                                   String attemptId,
                                                                   DqlRecoveryAttemptResultEnum expectedResult,
                                                                   boolean start) {
        DqlEventEntity event = mongoTemplate.findOne(
                Query.query(Criteria.where(DqlEventDto.FIELD_EVENT_ID).is(eventId)), entityClass);
        if (event == null || event.getRecoveryAttempts() == null) {
            return DqlRecoveryCallbackResultEnum.NOT_IN_BATCH;
        }
        List<DqlRecoveryAttemptDto> matches = event.getRecoveryAttempts().stream()
                .filter(item -> StringUtils.equals(batchId, item.getBatchId()))
                .filter(item -> StringUtils.equals(attemptId, item.getAttemptId()))
                .toList();
        if (matches.isEmpty()) {
            return DqlRecoveryCallbackResultEnum.NOT_IN_BATCH;
        }
        if (start) {
            return DqlRecoveryCallbackResultEnum.DUPLICATE;
        }
        boolean expected = matches.stream().anyMatch(item -> StringUtils.equals(
                expectedResult.name(), item.getResult()));
        if (expected) {
            return DqlRecoveryCallbackResultEnum.DUPLICATE;
        }
        boolean terminal = matches.stream().anyMatch(item -> terminalAttemptResults().contains(item.getResult()));
        return terminal ? DqlRecoveryCallbackResultEnum.CONFLICT : DqlRecoveryCallbackResultEnum.NOT_IN_BATCH;
    }

    private Criteria attemptIdentityCriteria(String batchId, String attemptId) {
        return Criteria.where(DqlRecoveryAttemptDto.FIELD_BATCH_ID).is(batchId)
                .and(DqlRecoveryAttemptDto.FIELD_ATTEMPT_ID).is(attemptId);
    }

    private List<String> terminalAttemptResults() {
        return List.of(
                DqlRecoveryAttemptResultEnum.SUCCESS.name(),
                DqlRecoveryAttemptResultEnum.FAILED.name(),
                DqlRecoveryAttemptResultEnum.SKIPPED.name(),
                DqlRecoveryAttemptResultEnum.TIMEOUT.name());
    }

    private boolean modified(UpdateResult result) {
        return result != null && result.getModifiedCount() > 0;
    }

    private long modifiedCount(UpdateResult result) {
        return result == null ? 0 : result.getModifiedCount();
    }

    public long releaseBatchLocks(String batchId, DqlEventStatusEnum targetStatus) {
        return releaseBatchLocks(batchId, null, targetStatus);
    }

    public long releaseBatchLocks(String batchId, Map<String, DqlEventStatusEnum> targetStatuses) {
        if (StringUtils.isBlank(batchId) || targetStatuses == null || targetStatuses.isEmpty()) {
            return 0;
        }
        Map<DqlEventStatusEnum, List<String>> eventIdsByStatus = new EnumMap<>(DqlEventStatusEnum.class);
        targetStatuses.forEach((eventId, targetStatus) -> {
            if (StringUtils.isNotBlank(eventId) && targetStatus != null && targetStatus.reprocessable()) {
                eventIdsByStatus.computeIfAbsent(targetStatus, ignored -> new ArrayList<>()).add(eventId);
            }
        });
        long released = 0;
        for (Map.Entry<DqlEventStatusEnum, List<String>> entry : eventIdsByStatus.entrySet()) {
            released += releaseBatchLocks(batchId, entry.getValue(), entry.getKey());
        }
        return released;
    }

    private long releaseBatchLocks(String batchId, List<String> eventIds, DqlEventStatusEnum targetStatus) {
        Criteria criteria = Criteria.where(DqlEventDto.FIELD_CURRENT_BATCH_ID).is(batchId)
                .and(DqlEventDto.FIELD_STATUS).is(DqlEventStatusEnum.REPROCESSING.name());
        if (eventIds != null && !eventIds.isEmpty()) {
            criteria.and(DqlEventDto.FIELD_EVENT_ID).in(eventIds);
        }
        Query query = Query.query(criteria);
        Date now = new Date();
        Update update = new Update()
                .set(DqlEventDto.FIELD_STATUS, targetStatus.name())
                .set(DqlEventDto.FIELD_CURRENT_BATCH_ID, null)
                .set(DqlEventDto.FIELD_UPDATED, now)
                .set(DqlEventDto.FIELD_TTL_AT, now);
        return mongoTemplate.updateMulti(query, update, entityClass).getModifiedCount();
    }

    private Query batchEventQuery(String eventId, String batchId) {
        return Query.query(Criteria.where(DqlEventDto.FIELD_EVENT_ID).is(eventId)
                .and(DqlEventDto.FIELD_STATUS).is(DqlEventStatusEnum.REPROCESSING.name())
                .and(DqlEventDto.FIELD_CURRENT_BATCH_ID).is(batchId));
    }

    private Criteria uniqueCriteria(DqlEventDto dto) {
        if (StringUtils.isNotBlank(dto.getEventIdentity())) {
            return Criteria.where(DqlEventDto.FIELD_TASK_ID).is(dto.getTaskId())
                    .and(DqlEventDto.FIELD_TASK_RECORD_ID).is(dto.getTaskRecordId())
                    .and(DqlEventDto.FIELD_TABLE_ID).is(dto.getTableId())
                    .and(DqlEventDto.FIELD_EVENT_IDENTITY).is(dto.getEventIdentity())
                    .and(DqlEventDto.FIELD_FAILED_NODE_ID).is(dto.getFailedNodeId());
        }
        return Criteria.where(DqlEventDto.FIELD_EVENT_ID).is(dto.getEventId());
    }

    private Criteria buildCriteria(com.tapdata.tm.dql.vo.DqlEventQueryVo queryVo) {
        return buildCriteria(queryVo, null);
    }

    private Criteria buildCriteria(com.tapdata.tm.dql.vo.DqlEventQueryVo queryVo, Collection<String> visibleTaskIds) {
        List<Criteria> criteria = new ArrayList<>();
        if (visibleTaskIds != null) {
            Collection<String> scopedTaskIds = visibleTaskIds.isEmpty()
                    ? List.of("__NO_VISIBLE_TASK__")
                    : visibleTaskIds;
            criteria.add(Criteria.where(DqlEventDto.FIELD_TASK_ID).in(scopedTaskIds));
        }
        if (queryVo != null) {
            addEquals(criteria, DqlEventDto.FIELD_TASK_ID, queryVo.getTaskId());
            addEquals(criteria, DqlEventDto.FIELD_EVENT_ID, queryVo.getEventId());
            addRegex(criteria, DqlEventDto.FIELD_TASK_NAME, queryVo.getTaskName());
            addRegex(criteria, DqlEventDto.FIELD_SOURCE_TABLE, queryVo.getSourceTable());
            addRegex(criteria, DqlEventDto.FIELD_TARGET_TABLE, queryVo.getTargetTable());
            addEquals(criteria, DqlEventDto.FIELD_DML_TYPE, queryVo.getDmlType());
            addEquals(criteria, DqlEventDto.FIELD_ERROR_TYPE, queryVo.getErrorType());
            addEquals(criteria, DqlEventDto.FIELD_STATUS, queryVo.getStatus());
            addEquals(criteria, DqlEventDto.FIELD_ERROR_CODE, queryVo.getErrorCode());
            if (queryVo.getStartTime() != null || queryVo.getEndTime() != null) {
                Criteria failedAt = Criteria.where(DqlEventDto.FIELD_FAILED_AT);
                if (queryVo.getStartTime() != null) {
                    failedAt.gte(queryVo.getStartTime());
                }
                if (queryVo.getEndTime() != null) {
                    failedAt.lte(queryVo.getEndTime());
                }
                criteria.add(failedAt);
            }
            if (StringUtils.isNotBlank(queryVo.getKeyword())) {
                String regex = Pattern.quote(queryVo.getKeyword());
                criteria.add(new Criteria().orOperator(
//                        Criteria.where(DqlEventDto.FIELD_EVENT_ID).regex(regex, "i"),
//                        Criteria.where(DqlEventDto.FIELD_TASK_NAME).regex(regex, "i"),
//                        Criteria.where(DqlEventDto.FIELD_SOURCE_TABLE).regex(regex, "i"),
//                        Criteria.where(DqlEventDto.FIELD_TARGET_TABLE).regex(regex, "i"),
//                        Criteria.where(DqlEventDto.FIELD_ERROR_CODE).regex(regex, "i"),
                        Criteria.where(DqlEventDto.FIELD_RECORD_IDENTITY).regex(regex, "i"),
                        Criteria.where(DqlEventDto.FIELD_ROUTE_DECISION).regex(regex, "i"),
                        Criteria.where(DqlEventDto.FIELD_CLASSIFICATION_REASON).regex(regex, "i"),
                        Criteria.where(DqlEventDto.FIELD_ERROR_DETAILS).regex(regex, "i")
                ));
            }
        }
        if (criteria.isEmpty()) {
            return new Criteria();
        }
        return new Criteria().andOperator(criteria.toArray(new Criteria[0]));
    }

    private void addEquals(List<Criteria> criteria, String field, String value) {
        if (StringUtils.isNotBlank(value)) {
            criteria.add(Criteria.where(field).is(value));
        }
    }

    private void addRegex(List<Criteria> criteria, String field, String value) {
        if (StringUtils.isNotBlank(value)) {
            criteria.add(Criteria.where(field).regex(Pattern.quote(value), "i"));
        }
    }

    private Sort parseSort(String order) {
        if (StringUtils.isBlank(order)) {
            return Sort.by(Sort.Direction.DESC, DqlEventDto.FIELD_FAILED_AT);
        }
        List<Sort.Order> orders = new ArrayList<>();
        for (String item : order.split(",")) {
            Sort.Order parsed = parseOrder(item);
            if (parsed != null) {
                orders.add(parsed);
            }
        }
        return orders.isEmpty() ? Sort.by(Sort.Direction.DESC, DqlEventDto.FIELD_FAILED_AT) : Sort.by(orders);
    }

    private Sort.Order parseOrder(String item) {
        if (StringUtils.isBlank(item)) {
            return null;
        }
        String trimmed = item.trim();
        Sort.Direction direction = Sort.Direction.ASC;
        String field = trimmed;
        if (trimmed.startsWith("-") && trimmed.length() > 1) {
            direction = Sort.Direction.DESC;
            field = trimmed.substring(1).trim();
        } else {
            String[] parts = trimmed.split("\\s+");
            field = parts[0];
            if (parts.length > 1) {
                direction = Sort.Direction.fromOptionalString(parts[1]).orElse(Sort.Direction.ASC);
            }
        }
        if (StringUtils.isBlank(field)) {
            return null;
        }
        return new Sort.Order(direction, toMongoField(field));
    }

    private String toMongoField(String field) {
        if ("eventId".equals(field)) return DqlEventDto.FIELD_EVENT_ID;
        if ("taskId".equals(field)) return DqlEventDto.FIELD_TASK_ID;
        if ("taskName".equals(field)) return DqlEventDto.FIELD_TASK_NAME;
        if ("sourceTable".equals(field)) return DqlEventDto.FIELD_SOURCE_TABLE;
        if ("targetTable".equals(field)) return DqlEventDto.FIELD_TARGET_TABLE;
        if ("dmlType".equals(field)) return DqlEventDto.FIELD_DML_TYPE;
        if ("failedAt".equals(field)) return DqlEventDto.FIELD_FAILED_AT;
        if ("eventTime".equals(field)) return DqlEventDto.FIELD_EVENT_TIME;
        if ("captureSeq".equals(field)) return DqlEventDto.FIELD_CAPTURE_SEQ;
        if ("errorType".equals(field)) return DqlEventDto.FIELD_ERROR_TYPE;
        if ("routeDecision".equals(field)) return DqlEventDto.FIELD_ROUTE_DECISION;
        if ("classificationReason".equals(field)) return DqlEventDto.FIELD_CLASSIFICATION_REASON;
        if ("recoveryCount".equals(field)) return DqlEventDto.FIELD_RECOVERY_COUNT;
        if ("lastRecoveryTime".equals(field)) return DqlEventDto.FIELD_LAST_RECOVERY_TIME;
        if ("recordIdentity".equals(field)) return DqlEventDto.FIELD_RECORD_IDENTITY;
        if ("overwriteRisk".equals(field)) return DqlEventDto.FIELD_OVERWRITE_RISK;
        if ("laterSuccessAt".equals(field)) return DqlEventDto.FIELD_LATER_SUCCESS_AT;
        if ("created".equals(field)) return DqlEventDto.FIELD_CREATED;
        if ("updated".equals(field)) return DqlEventDto.FIELD_UPDATED;
        return field;
    }

    private void setOnInsert(Update update, String field, Object value) {
        if (value != null) {
            update.setOnInsert(field, value);
        }
    }

    public DqlEventDto convert(DqlEventEntity entity) {
        if (entity == null) {
            return null;
        }
        DqlEventDto dto = new DqlEventDto();
        BeanUtils.copyProperties(entity, dto);
        if (entity.getId() != null) {
            dto.setId(entity.getId().toHexString());
        }
        return dto;
    }

}
