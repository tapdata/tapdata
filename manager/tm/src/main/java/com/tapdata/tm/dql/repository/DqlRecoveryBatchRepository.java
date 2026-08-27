package com.tapdata.tm.dql.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.dql.DqlRecoveryBatchStatusEnum;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
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
        dto.setSelectedCount(Optional.ofNullable(dto.getSelectedCount()).orElse(0));
        dto.setSuccessCount(Optional.ofNullable(dto.getSuccessCount()).orElse(0));
        dto.setFailedCount(Optional.ofNullable(dto.getFailedCount()).orElse(0));
        dto.setSkippedCount(Optional.ofNullable(dto.getSkippedCount()).orElse(0));
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

    public void markRunning(String batchId) {
        Date now = new Date();
        Update update = new Update()
                .set(DqlRecoveryBatchDto.FIELD_STATUS, DqlRecoveryBatchStatusEnum.RUNNING.name())
                .set(DqlRecoveryBatchDto.FIELD_STARTED_AT, now)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        mongoTemplate.updateFirst(batchQuery(batchId,
                DqlRecoveryBatchStatusEnum.DISPATCHED), update, entityClass);
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
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        if (message != null) {
            update.set(DqlRecoveryBatchDto.FIELD_MESSAGE, message);
        }
        UpdateResult result = mongoTemplate.updateFirst(
                batchQuery(batchId, ACTIVE_STATUSES), update, entityClass);
        return result.getModifiedCount() > 0;
    }

    public void increaseSkipped(String batchId) {
        increase(batchId, DqlRecoveryBatchDto.FIELD_SKIPPED_COUNT);
    }

    public void finish(String batchId, DqlRecoveryBatchStatusEnum status, String message) {
        List<DqlRecoveryBatchStatusEnum> sourceStatuses = finishSourceStatuses(status);
        Date now = new Date();
        Update update = new Update()
                .set(DqlRecoveryBatchDto.FIELD_STATUS, status.name())
                .set(DqlRecoveryBatchDto.FIELD_FINISHED_AT, now)
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
            case SUCCESS -> List.of(DqlRecoveryBatchStatusEnum.RUNNING);
            case PARTIAL_FAILED -> List.of(
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
