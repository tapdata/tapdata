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
import java.util.Optional;

@Repository
public class DqlRecoveryBatchRepository {
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
        dto.setTtlAt(Optional.ofNullable(dto.getTtlAt()).orElse(dto.getCreated()));
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

    public void updateStatus(String batchId, DqlRecoveryBatchStatusEnum status, String message) {
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

    public void increaseSkipped(String batchId) {
        increase(batchId, DqlRecoveryBatchDto.FIELD_SKIPPED_COUNT);
    }

    public void finish(String batchId, DqlRecoveryBatchStatusEnum status, String message) {
        Date now = new Date();
        Update update = new Update()
                .set(DqlRecoveryBatchDto.FIELD_STATUS, status.name())
                .set(DqlRecoveryBatchDto.FIELD_FINISHED_AT, now)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        if (message != null) {
            update.set(DqlRecoveryBatchDto.FIELD_MESSAGE, message);
        }
        mongoTemplate.updateFirst(batchQuery(batchId,
                DqlRecoveryBatchStatusEnum.CREATED,
                DqlRecoveryBatchStatusEnum.DISPATCHED,
                DqlRecoveryBatchStatusEnum.RUNNING), update, entityClass);
    }

    private void increase(String batchId, String field) {
        Date now = new Date();
        Update update = new Update()
                .inc(field, 1)
                .set(DqlRecoveryBatchDto.FIELD_UPDATED, now)
                .set(DqlRecoveryBatchDto.FIELD_TTL_AT, now);
        mongoTemplate.updateFirst(batchQuery(batchId,
                DqlRecoveryBatchStatusEnum.CREATED,
                DqlRecoveryBatchStatusEnum.DISPATCHED,
                DqlRecoveryBatchStatusEnum.RUNNING), update, entityClass);
    }

    private Query batchQuery(String batchId, DqlRecoveryBatchStatusEnum... statuses) {
        Criteria criteria = Criteria.where(DqlRecoveryBatchDto.FIELD_BATCH_ID).is(batchId);
        if (statuses != null && statuses.length > 0) {
            criteria.and(DqlRecoveryBatchDto.FIELD_STATUS).in(
                    java.util.Arrays.stream(statuses).map(Enum::name).toList());
        }
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
