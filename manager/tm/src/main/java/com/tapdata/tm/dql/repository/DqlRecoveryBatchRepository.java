package com.tapdata.tm.dql.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.dql.DqlRecoveryBatchStatusEnum;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAuditEntryDto;
import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import com.tapdata.tm.dql.dto.DqlRecoveryNodeStateDto;
import com.tapdata.tm.dql.entity.DqlRecoveryBatchEntity;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Repository
public class DqlRecoveryBatchRepository {
    private static final List<DqlRecoveryBatchStatusEnum> ACTIVE_STATUSES = List.of(
            DqlRecoveryBatchStatusEnum.CREATED,
            DqlRecoveryBatchStatusEnum.DISPATCHED,
            DqlRecoveryBatchStatusEnum.RUNNING
    );
    private final MongoTemplate mongoTemplate;
    private final Class<DqlRecoveryBatchEntity> entityClass;
    private final String collectionName;

    public DqlRecoveryBatchRepository(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
        this.entityClass = DqlRecoveryBatchEntity.class;
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
        mongoTemplate.indexOps(collectionName).createIndex(new Index()
                .on(DqlRecoveryBatchDto.FIELD_BATCH_ID, Sort.Direction.ASC)
                .unique()
                .named("uk_batch_id"));
        mongoTemplate.indexOps(collectionName).createIndex(new Index()
                .on(DqlRecoveryBatchDto.FIELD_TASK_ID, Sort.Direction.ASC)
                .on(DqlRecoveryBatchDto.FIELD_CREATED, Sort.Direction.DESC)
                .named("idx_task_created"));
        mongoTemplate.indexOps(collectionName).createIndex(new Index()
                .on(DqlRecoveryBatchDto.FIELD_STATUS, Sort.Direction.ASC)
                .on(DqlRecoveryBatchDto.FIELD_CREATED, Sort.Direction.DESC)
                .named("idx_status_created"));
    }

    public DqlRecoveryBatchDto create(DqlRecoveryBatchDto dto) {
        Date now = new Date();
        dto.setCreated(Optional.ofNullable(dto.getCreated()).orElse(now));
        dto.setUpdated(now);
        dto.setTtlAt(dto.getCreated());
        dto.setStatus(Optional.ofNullable(dto.getStatus()).orElse(DqlRecoveryBatchStatusEnum.CREATED.name()));
        dto.setMode(Optional.ofNullable(dto.getMode()).orElse(DqlRecoveryMessageDto.MODE_AUTO));
        dto.setAuditEntries(Optional.ofNullable(dto.getAuditEntries()).orElseGet(ArrayList::new));
        dto.setSelectedCount(Optional.ofNullable(dto.getSelectedCount()).orElse(0));
        dto.setSuccessCount(Optional.ofNullable(dto.getSuccessCount()).orElse(0));
        dto.setFailedCount(Optional.ofNullable(dto.getFailedCount()).orElse(0));
        dto.setSkippedCount(Optional.ofNullable(dto.getSkippedCount()).orElse(0));
        dto.setFinishRequested(Optional.ofNullable(dto.getFinishRequested()).orElse(false));
        DqlRecoveryBatchEntity saved = mongoTemplate.save(convert(dto));
        return convert(saved);
    }

    public DqlRecoveryBatchDto findByBatchId(String batchId) {
        if (StringUtils.isBlank(batchId)) {
            return null;
        }
        Query query = Query.query(Criteria.where(DqlRecoveryBatchDto.FIELD_BATCH_ID).is(batchId));
        return convert(mongoTemplate.findOne(query, entityClass));
    }

    /** Returns the newest non-terminal batch for a task, if one exists. */
    public DqlRecoveryBatchDto findActiveByTaskId(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return null;
        }
        Query query = batchQueryByTask(taskId, ACTIVE_STATUSES)
                .with(Sort.by(Sort.Order.desc(DqlRecoveryBatchDto.FIELD_CREATED)));
        return convert(mongoTemplate.findOne(query, entityClass));
    }

    public List<DqlRecoveryBatchDto> findTimedOut(Date deadline) {
        if (deadline == null) {
            return List.of();
        }
        Query query = Query.query(Criteria.where(DqlRecoveryBatchDto.FIELD_STATUS).in(
                        DqlRecoveryBatchStatusEnum.DISPATCHED.name(),
                        DqlRecoveryBatchStatusEnum.RUNNING.name())
                .and(DqlRecoveryBatchDto.FIELD_UPDATED).lte(deadline))
                .with(Sort.by(Sort.Order.asc(DqlRecoveryBatchDto.FIELD_UPDATED)));
        return mongoTemplate.find(query, entityClass).stream().map(this::convert).toList();
    }

    public List<DqlRecoveryBatchDto> findTimedOut(Date dispatchDeadline,
                                                  Date heartbeatDeadline,
                                                  Date legacyDeadline) {
        if (dispatchDeadline == null || heartbeatDeadline == null || legacyDeadline == null) {
            return List.of();
        }
        Query query = Query.query(timedOutCriteria(dispatchDeadline, heartbeatDeadline, legacyDeadline))
                .with(Sort.by(Sort.Order.asc(DqlRecoveryBatchDto.FIELD_UPDATED)));
        return mongoTemplate.find(query, entityClass).stream().map(this::convert).toList();
    }

    public void updateStatus(String batchId, DqlRecoveryBatchStatusEnum status, String message) {
        if (status != DqlRecoveryBatchStatusEnum.DISPATCHED) {
            throw new IllegalArgumentException("Only CREATED -> DISPATCHED is supported by updateStatus");
        }
        Date now = new Date();
        Update update = new Update()
                .set(DqlRecoveryBatchDto.FIELD_STATUS, status.name())
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        if (message != null) {
            update.set(DqlRecoveryBatchDto.FIELD_MESSAGE, message);
        }
        mongoTemplate.updateFirst(batchQuery(batchId,
                DqlRecoveryBatchStatusEnum.CREATED), update, entityClass);
    }

    public boolean markRunning(String batchId) {
        Date now = new Date();
        Update update = new Update()
                .set(DqlRecoveryBatchDto.FIELD_STATUS, DqlRecoveryBatchStatusEnum.RUNNING.name())
                .set(DqlRecoveryBatchDto.FIELD_STARTED_AT, now)
                .set(DqlRecoveryBatchDto.FIELD_PING_TIME, now)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        UpdateResult result = mongoTemplate.updateFirst(batchQuery(batchId,
                DqlRecoveryBatchStatusEnum.CREATED, DqlRecoveryBatchStatusEnum.DISPATCHED), update, entityClass);
        return result.getModifiedCount() > 0;
    }

    public boolean touchHeartbeat(String batchId, Date pingTime) {
        if (StringUtils.isBlank(batchId) || pingTime == null) {
            return false;
        }
        Update update = new Update()
                .set(DqlRecoveryBatchDto.FIELD_PING_TIME, pingTime)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, pingTime)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, pingTime);
        UpdateResult result = mongoTemplate.updateFirst(
                batchQuery(batchId, DqlRecoveryBatchStatusEnum.RUNNING), update, entityClass);
        return result.getModifiedCount() > 0;
    }

    public void increaseSuccess(String batchId) {
        increase(batchId, DqlRecoveryBatchDto.FIELD_SUCCESS_COUNT);
    }

    public void increaseFailed(String batchId) {
        increase(batchId, DqlRecoveryBatchDto.FIELD_FAILED_COUNT);
    }

    public void increaseFailed(String batchId, int count) {
        if (count > 0) {
            increase(batchId, DqlRecoveryBatchDto.FIELD_FAILED_COUNT, count);
        }
    }

    public boolean finishTimedOut(String batchId, DqlRecoveryBatchStatusEnum status, String message) {
        if (status != DqlRecoveryBatchStatusEnum.FAILED
                && status != DqlRecoveryBatchStatusEnum.PARTIAL_FAILED) {
            throw new IllegalArgumentException("Timeout status must be FAILED or PARTIAL_FAILED");
        }
        Date now = new Date();
        Update update = new Update()
                .set(DqlRecoveryBatchDto.FIELD_STATUS, status.name())
                .set(DqlRecoveryBatchDto.FIELD_FINISHED_AT, now)
                .set(DqlRecoveryBatchDto.FIELD_FINISH_REQUESTED, false)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        if (message != null) {
            update.set(DqlRecoveryBatchDto.FIELD_MESSAGE, message);
        }
        UpdateResult result = mongoTemplate.updateFirst(
                batchQuery(batchId, ACTIVE_STATUSES), update, entityClass);
        return result.getModifiedCount() > 0;
    }

    /**
     * Finishes a batch only if it still satisfies the timeout predicate used
     * by the scan. A heartbeat can arrive after the scan query but before the
     * final update; rechecking here prevents that late scan from terminating
     * a batch that has become live again.
     */
    public boolean finishTimedOut(String batchId,
                                  DqlRecoveryBatchStatusEnum status,
                                  String message,
                                  Date dispatchDeadline,
                                  Date heartbeatDeadline,
                                  Date legacyDeadline) {
        if (StringUtils.isBlank(batchId)
                || dispatchDeadline == null
                || heartbeatDeadline == null
                || legacyDeadline == null) {
            return false;
        }
        if (status != DqlRecoveryBatchStatusEnum.FAILED
                && status != DqlRecoveryBatchStatusEnum.PARTIAL_FAILED) {
            throw new IllegalArgumentException("Timeout status must be FAILED or PARTIAL_FAILED");
        }
        Date now = new Date();
        Update update = new Update()
                .set(DqlRecoveryBatchDto.FIELD_STATUS, status.name())
                .set(DqlRecoveryBatchDto.FIELD_FINISHED_AT, now)
                .set(DqlRecoveryBatchDto.FIELD_FINISH_REQUESTED, false)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        if (message != null) {
            update.set(DqlRecoveryBatchDto.FIELD_MESSAGE, message);
        }
        Criteria criteria = new Criteria().andOperator(
                Criteria.where(DqlRecoveryBatchDto.FIELD_BATCH_ID).is(batchId),
                timedOutCriteria(dispatchDeadline, heartbeatDeadline, legacyDeadline));
        UpdateResult result = mongoTemplate.updateFirst(
                Query.query(criteria), update, entityClass);
        return result.getModifiedCount() > 0;
    }

    public void increaseSkipped(String batchId) {
        increase(batchId, DqlRecoveryBatchDto.FIELD_SKIPPED_COUNT);
    }

    /**
     * Records a finish callback that arrived before all event counter updates
     * became visible. The callback is completed by the next event result.
     */
    public void recordFinishRequested(String batchId, String message) {
        Date now = new Date();
        Update update = new Update()
                .set(DqlRecoveryBatchDto.FIELD_FINISH_REQUESTED, true)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        if (message != null) {
            update.set(DqlRecoveryBatchDto.FIELD_FINISH_MESSAGE, message);
        }
        mongoTemplate.updateFirst(batchQuery(batchId, ACTIVE_STATUSES), update, entityClass);
    }

    /**
     * Atomically consumes a pending finish request after event counters have
     * reconciled. This prevents two concurrent event callbacks from both
     * finalizing the same batch or raising duplicate alarms.
     */
    public boolean finishRequested(String batchId,
                                   DqlRecoveryBatchStatusEnum status,
                                   String message) {
        if (StringUtils.isBlank(batchId) || status == null) {
            return false;
        }
        List<DqlRecoveryBatchStatusEnum> sourceStatuses = finishSourceStatuses(status);
        Date now = new Date();
        Update update = new Update()
                .set(DqlRecoveryBatchDto.FIELD_STATUS, status.name())
                .set(DqlRecoveryBatchDto.FIELD_FINISHED_AT, now)
                .set(DqlRecoveryBatchDto.FIELD_FINISH_REQUESTED, false)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        if (message != null) {
            update.set(DqlRecoveryBatchDto.FIELD_MESSAGE, message);
        }
        Query query = batchQuery(batchId, sourceStatuses);
        query.addCriteria(Criteria.where(DqlRecoveryBatchDto.FIELD_FINISH_REQUESTED).is(true));
        UpdateResult result = mongoTemplate.updateFirst(query, update, entityClass);
        return result.getModifiedCount() > 0;
    }

    /**
     * Repairs a legacy active batch for which every selected event already
     * has a terminal counter, but the batch-finished callback was lost. The
     * counter predicates make the repair safe against a still-running batch
     * and keep it atomic with the terminal transition.
     */
    public boolean finishReconciled(String batchId,
                                    DqlRecoveryBatchStatusEnum status,
                                    int selected,
                                    int success,
                                    int failed,
                                    int skipped,
                                    String message) {
        if (StringUtils.isBlank(batchId)
                || status == null
                || selected <= 0
                || success < 0
                || failed < 0
                || skipped < 0
                || selected != success + failed + skipped) {
            return false;
        }
        if (status != DqlRecoveryBatchStatusEnum.SUCCESS
                && status != DqlRecoveryBatchStatusEnum.PARTIAL_FAILED) {
            return false;
        }
        if (status == DqlRecoveryBatchStatusEnum.SUCCESS && (failed > 0 || skipped > 0)) {
            return false;
        }
        if (status == DqlRecoveryBatchStatusEnum.PARTIAL_FAILED && failed == 0 && skipped == 0) {
            return false;
        }
        Date now = new Date();
        Update update = new Update()
                .set(DqlRecoveryBatchDto.FIELD_STATUS, status.name())
                .set(DqlRecoveryBatchDto.FIELD_FINISHED_AT, now)
                .set(DqlRecoveryBatchDto.FIELD_FINISH_REQUESTED, false)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        if (message != null) {
            update.set(DqlRecoveryBatchDto.FIELD_MESSAGE, message);
        }
        Query query = batchQuery(batchId, finishSourceStatuses(status));
        query.addCriteria(Criteria.where(DqlRecoveryBatchDto.FIELD_SELECTED_COUNT).is(selected));
        query.addCriteria(Criteria.where(DqlRecoveryBatchDto.FIELD_SUCCESS_COUNT).is(success));
        query.addCriteria(Criteria.where(DqlRecoveryBatchDto.FIELD_FAILED_COUNT).is(failed));
        query.addCriteria(Criteria.where(DqlRecoveryBatchDto.FIELD_SKIPPED_COUNT).is(skipped));
        UpdateResult result = mongoTemplate.updateFirst(query, update, entityClass);
        return result.getModifiedCount() > 0;
    }

    public void appendAudit(String batchId, DqlRecoveryAuditEntryDto entry) {
        if (entry == null) {
            return;
        }
        Date now = Optional.ofNullable(entry.getOccurredAt()).orElseGet(Date::new);
        entry.setOccurredAt(now);
        Update update = new Update()
                .push(DqlRecoveryBatchDto.FIELD_AUDIT_ENTRIES, entry)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        mongoTemplate.updateFirst(batchQuery(batchId), update, entityClass);
    }

    public void updateNodeStates(String batchId, List<DqlRecoveryNodeStateDto> nodeStates) {
        if (StringUtils.isBlank(batchId) || nodeStates == null) {
            return;
        }
        Date now = new Date();
        mongoTemplate.updateFirst(batchQuery(batchId), new Update()
                .set(DqlRecoveryBatchDto.FIELD_NODE_STATES, nodeStates)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now), entityClass);
    }

    public void recordSourceReadResult(String batchId,
                                       boolean pause,
                                       String result,
                                       String message,
                                       Date occurredAt,
                                       DqlRecoveryAuditEntryDto auditEntry) {
        Date now = occurredAt == null ? new Date() : occurredAt;
        Update update = new Update()
                .set(pause ? DqlRecoveryBatchDto.FIELD_SOURCE_READ_PAUSE_RESULT
                        : DqlRecoveryBatchDto.FIELD_SOURCE_READ_RESUME_RESULT, result)
                .set(pause ? DqlRecoveryBatchDto.FIELD_SOURCE_READ_PAUSE_AT
                        : DqlRecoveryBatchDto.FIELD_SOURCE_READ_RESUME_AT, now)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        if (message != null) {
            update.set(pause ? DqlRecoveryBatchDto.FIELD_SOURCE_READ_PAUSE_MESSAGE
                    : DqlRecoveryBatchDto.FIELD_SOURCE_READ_RESUME_MESSAGE, message);
        }
        if (auditEntry != null) {
            auditEntry.setOccurredAt(Optional.ofNullable(auditEntry.getOccurredAt()).orElse(now));
            update.push(DqlRecoveryBatchDto.FIELD_AUDIT_ENTRIES, auditEntry);
        }
        mongoTemplate.updateFirst(batchQuery(batchId), update, entityClass);
    }

    public void finish(String batchId, DqlRecoveryBatchStatusEnum status, String message) {
        List<DqlRecoveryBatchStatusEnum> sourceStatuses = finishSourceStatuses(status);
        Date now = new Date();
        Update update = new Update()
                .set(DqlRecoveryBatchDto.FIELD_STATUS, status.name())
                .set(DqlRecoveryBatchDto.FIELD_FINISHED_AT, now)
                .set(DqlRecoveryBatchDto.FIELD_FINISH_REQUESTED, false)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        if (message != null) {
            update.set(DqlRecoveryBatchDto.FIELD_MESSAGE, message);
        }
        mongoTemplate.updateFirst(batchQuery(batchId, sourceStatuses), update, entityClass);
    }

    private List<DqlRecoveryBatchStatusEnum> finishSourceStatuses(DqlRecoveryBatchStatusEnum status) {
        if (status == null) {
            throw new IllegalArgumentException("Finish status must not be null");
        }
        return switch (status) {
            case SUCCESS -> List.of(
                    DqlRecoveryBatchStatusEnum.CREATED,
                    DqlRecoveryBatchStatusEnum.DISPATCHED,
                    DqlRecoveryBatchStatusEnum.RUNNING);
            case PARTIAL_FAILED -> List.of(
                    DqlRecoveryBatchStatusEnum.CREATED,
                    DqlRecoveryBatchStatusEnum.DISPATCHED,
                    DqlRecoveryBatchStatusEnum.RUNNING);
            case FAILED -> ACTIVE_STATUSES;
            case CANCELED -> List.of(DqlRecoveryBatchStatusEnum.CREATED);
            default -> throw new IllegalArgumentException("Finish status must be terminal: " + status);
        };
    }

    private void increase(String batchId, String field) {
        increase(batchId, field, 1);
    }

    private void increase(String batchId, String field, int amount) {
        Date now = new Date();
        Update update = new Update()
                .inc(field, amount)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        mongoTemplate.updateFirst(batchQuery(batchId, ACTIVE_STATUSES), update, entityClass);
    }

    private Query batchQuery(String batchId, DqlRecoveryBatchStatusEnum... statuses) {
        Criteria criteria = Criteria.where(DqlRecoveryBatchDto.FIELD_BATCH_ID).is(batchId);
        if (statuses != null && statuses.length > 0) {
            criteria.and(DqlRecoveryBatchDto.FIELD_STATUS).in(Arrays.stream(statuses).map(Enum::name).toList());
        }
        return Query.query(criteria);
    }

    private Query batchQuery(String batchId, List<DqlRecoveryBatchStatusEnum> statuses) {
        return batchQuery(batchId, statuses.toArray(new DqlRecoveryBatchStatusEnum[0]));
    }

    private Query batchQueryByTask(String taskId, List<DqlRecoveryBatchStatusEnum> statuses) {
        Criteria criteria = Criteria.where(DqlRecoveryBatchDto.FIELD_TASK_ID).is(taskId)
                .and(DqlRecoveryBatchDto.FIELD_STATUS).in(
                        statuses.stream().map(Enum::name).collect(Collectors.toList()));
        return Query.query(criteria);
    }

    private Criteria timedOutCriteria(Date dispatchDeadline,
                                      Date heartbeatDeadline,
                                      Date legacyDeadline) {
        Criteria dispatched = Criteria.where(DqlRecoveryBatchDto.FIELD_STATUS)
                .is(DqlRecoveryBatchStatusEnum.DISPATCHED.name())
                .and(DqlRecoveryBatchDto.FIELD_UPDATED).lte(dispatchDeadline);
        Criteria running = new Criteria().andOperator(
                Criteria.where(DqlRecoveryBatchDto.FIELD_STATUS)
                        .is(DqlRecoveryBatchStatusEnum.RUNNING.name()),
                new Criteria().orOperator(
                        Criteria.where(DqlRecoveryBatchDto.FIELD_PING_TIME).lte(heartbeatDeadline),
                        new Criteria().andOperator(
                                new Criteria().orOperator(
                                        Criteria.where(DqlRecoveryBatchDto.FIELD_PING_TIME).exists(false),
                                        Criteria.where(DqlRecoveryBatchDto.FIELD_PING_TIME).is(null)),
                                Criteria.where(DqlRecoveryBatchDto.FIELD_UPDATED).lte(legacyDeadline))));
        return new Criteria().orOperator(dispatched, running);
    }

    public DqlRecoveryBatchDto convert(DqlRecoveryBatchEntity entity) {
        if (entity == null) {
            return null;
        }
        DqlRecoveryBatchDto dto = new DqlRecoveryBatchDto();
        BeanUtils.copyProperties(entity, dto);
        if (entity.getId() != null) {
            dto.setId(entity.getId().toHexString());
        }
        return dto;
    }

    private DqlRecoveryBatchEntity convert(DqlRecoveryBatchDto dto) {
        DqlRecoveryBatchEntity entity = new DqlRecoveryBatchEntity();
        BeanUtils.copyProperties(dto, entity);
        if (StringUtils.isNotBlank(dto.getId()) && ObjectId.isValid(dto.getId())) {
            entity.setId(new ObjectId(dto.getId()));
        }
        return entity;
    }
}
