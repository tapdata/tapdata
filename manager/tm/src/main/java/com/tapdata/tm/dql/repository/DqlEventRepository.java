package com.tapdata.tm.dql.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.DqlRecoveryAttemptResultEnum;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto;
import com.tapdata.tm.dql.entity.DqlEventEntity;
import com.tapdata.tm.dql.vo.DqlRecordSuccessReportVo;
import com.tapdata.tm.task.entity.TaskEntity;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.index.PartialIndexFilter;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
                .map(clz -> clz.getAnnotation(Document.class))
                .map(Document::value)
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
        Date ttlAt = Optional.ofNullable(dto.getTtlAt()).orElse(created);
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
        Query query = new Query(buildCriteria(queryVo));
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
        return mongoTemplate.count(new Query(buildCriteria(queryVo)), entityClass);
    }

    public long countByStatus(com.tapdata.tm.dql.vo.DqlEventQueryVo queryVo, DqlEventStatusEnum status) {
        com.tapdata.tm.dql.vo.DqlEventQueryVo scoped = new com.tapdata.tm.dql.vo.DqlEventQueryVo();
        if (queryVo != null) {
            BeanUtils.copyProperties(queryVo, scoped);
        }
        scoped.setStatus(status.name());
        return count(scoped);
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

    public long releaseBatchLocks(String batchId, DqlEventStatusEnum targetStatus) {
        Query query = Query.query(Criteria.where(DqlEventDto.FIELD_CURRENT_BATCH_ID).is(batchId)
                .and(DqlEventDto.FIELD_STATUS).is(DqlEventStatusEnum.REPROCESSING.name()));
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
        if (queryVo == null) {
            return new Criteria();
        }
        List<Criteria> criteria = new ArrayList<>();
        addEquals(criteria, DqlEventDto.FIELD_TASK_ID, queryVo.getTaskId());
        addRegex(criteria, DqlEventDto.FIELD_TASK_NAME, queryVo.getTaskName());
        addRegex(criteria, DqlEventDto.FIELD_SOURCE_TABLE, queryVo.getSourceTable());
        addRegex(criteria, DqlEventDto.FIELD_TARGET_TABLE, queryVo.getTargetTable());
        addEquals(criteria, DqlEventDto.FIELD_DML_TYPE, queryVo.getDmlType());
        addEquals(criteria, DqlEventDto.FIELD_ERROR_TYPE, queryVo.getErrorType());
        addEquals(criteria, DqlEventDto.FIELD_STATUS, queryVo.getStatus());
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
                    Criteria.where(DqlEventDto.FIELD_EVENT_ID).regex(regex, "i"),
                    Criteria.where(DqlEventDto.FIELD_TASK_NAME).regex(regex, "i"),
                    Criteria.where(DqlEventDto.FIELD_SOURCE_TABLE).regex(regex, "i"),
                    Criteria.where(DqlEventDto.FIELD_TARGET_TABLE).regex(regex, "i"),
                    Criteria.where(DqlEventDto.FIELD_ERROR_CODE).regex(regex, "i"),
                    Criteria.where(DqlEventDto.FIELD_RECORD_IDENTITY).regex(regex, "i"),
                    Criteria.where(DqlEventDto.FIELD_ROUTE_DECISION).regex(regex, "i"),
                    Criteria.where(DqlEventDto.FIELD_CLASSIFICATION_REASON).regex(regex, "i"),
                    Criteria.where(DqlEventDto.FIELD_ERROR_DETAILS).regex(regex, "i")
            ));
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
