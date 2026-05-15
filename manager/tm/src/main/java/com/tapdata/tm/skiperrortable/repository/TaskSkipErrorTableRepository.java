package com.tapdata.tm.skiperrortable.repository;

import com.mongodb.client.result.DeleteResult;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.skiperrortable.SkipErrorTableStatusEnum;
import com.tapdata.tm.skiperrortable.dto.TaskSkipErrorTableDto;
import com.tapdata.tm.skiperrortable.entity.TaskSkipErrorTableEntity;
import com.tapdata.tm.skiperrortable.vo.SkipErrorTableStatusVo;
import com.tapdata.tm.utils.MongoUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 复制任务跳过错误表-持久化操作
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/29 09:50 Create
 */
@Repository
public class TaskSkipErrorTableRepository {

    private final MongoTemplate mongoTemplate;
    private final String collectionName;
    private final Class<TaskSkipErrorTableEntity> entityClass;

    public TaskSkipErrorTableRepository(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
        this.entityClass = TaskSkipErrorTableEntity.class;
        this.collectionName = Optional.of(entityClass)
            .map(clz -> clz.getAnnotation(Document.class))
            .map(Document::value)
            .orElseThrow(() -> new IllegalArgumentException("Class " + entityClass.getSimpleName() + " is not a document"));
        init();
    }

    protected void init() {
        if (!mongoTemplate.collectionExists(collectionName)) {
            mongoTemplate.createCollection(collectionName);
            mongoTemplate.indexOps(collectionName).createIndex(new Index(TaskSkipErrorTableDto.FIELD_TASK_ID, Sort.Direction.ASC));
            mongoTemplate.indexOps(collectionName).createIndex(new Index(TaskSkipErrorTableDto.FIELD_SOURCE_TABLE, Sort.Direction.ASC));
            mongoTemplate.indexOps(collectionName).createIndex(new Index(TaskSkipErrorTableDto.FIELD_TARGET_TABLE, Sort.Direction.ASC));
            mongoTemplate.indexOps(collectionName).createIndex(new Index()
                .on(TaskSkipErrorTableDto.FIELD_TASK_ID, Sort.Direction.ASC)
                .on(TaskSkipErrorTableDto.FIELD_SOURCE_TABLE, Sort.Direction.ASC)
                .unique()
            );
        }
    }

    protected TaskSkipErrorTableDto convert(TaskSkipErrorTableEntity entity) {
        if (null == entity) return null;

        TaskSkipErrorTableDto dto = new TaskSkipErrorTableDto();
        if (null != entity.getId()) {
            dto.setId(entity.getId().toHexString());
        }
        dto.setStatus(SkipErrorTableStatusEnum.parse(entity.getStatus()));
        dto.setCreated(entity.getCreated());
        dto.setUpdated(entity.getUpdated());

        dto.setTaskId(entity.getTaskId());
        dto.setSourceTable(entity.getSourceTable());
        dto.setTargetTable(entity.getTargetTable());
        dto.setSkipStage(entity.getSkipStage());
        dto.setSkipDate(entity.getSkipDate());
        dto.setCdcDate(entity.getCdcDate());
        dto.setErrorCode(entity.getErrorCode());
        dto.setErrorMessage(entity.getErrorMessage());
        return dto;
    }

    public long deleteAll(Query query) {
        DeleteResult result = mongoTemplate.remove(query, entityClass);
        return result.getDeletedCount();
    }

    public TaskSkipErrorTableDto addSkipTable(TaskSkipErrorTableDto dto) {
        Assert.notNull(dto, "DTO must not be null!");
        Assert.notNull(dto.getTaskId(), "taskId must not be null!");
        Assert.notNull(dto.getSourceTable(), "sourceTable must not be null!");
        Assert.notNull(dto.getTargetTable(), "targetTable must not be null!");

        Date nowDate = new Date();

        Query query = Query.query(Criteria
            .where(TaskSkipErrorTableDto.FIELD_TASK_ID).is(dto.getTaskId())
            .and(TaskSkipErrorTableDto.FIELD_SOURCE_TABLE).is(dto.getSourceTable())
        );
        Update update = Update.update(TaskSkipErrorTableDto.FIELD_UPDATED, nowDate);
        update.set(TaskSkipErrorTableDto.FIELD_STATUS, SkipErrorTableStatusEnum.SKIPPED);
        update.set(TaskSkipErrorTableDto.FIELD_TARGET_TABLE, dto.getTargetTable());
        update.set(TaskSkipErrorTableDto.FIELD_SKIP_STAGE, dto.getSkipStage());
        update.set(TaskSkipErrorTableDto.FIELD_SKIP_DATE, dto.getSkipDate());
        update.set(TaskSkipErrorTableDto.FIELD_CDC_DATE, dto.getCdcDate());
        update.set(TaskSkipErrorTableDto.FIELD_ERROR_CODE, dto.getErrorCode());
        update.set(TaskSkipErrorTableDto.FIELD_ERROR_MESSAGE, dto.getErrorMessage());

        update.setOnInsert(TaskSkipErrorTableDto.FIELD_CREATED, nowDate);

        FindAndModifyOptions options = FindAndModifyOptions.options().returnNew(true).upsert(true);
        TaskSkipErrorTableEntity entity = mongoTemplate.findAndModify(query, update, options, entityClass);
        return convert(entity);
    }

    public Page<TaskSkipErrorTableDto> pageOfTaskId(String taskId, String tableFilter, long skip, int limit, String orderStr) {
        Assert.notNull(taskId, "taskId not null");
        skip = Math.max(0, skip);
        limit = Math.max(1, limit);
        Sort sort = Optional.ofNullable(MongoUtils.parseSort(orderStr))
            .orElse(Sort.by(Sort.Direction.ASC, TaskSkipErrorTableDto.FIELD_CREATED));

        Criteria criteria = Criteria.where(TaskSkipErrorTableDto.FIELD_TASK_ID).is(taskId);
        if (null != tableFilter) {
            criteria.orOperator(
                Criteria.where(TaskSkipErrorTableDto.FIELD_SOURCE_TABLE).regex(String.format(".*%s.*", tableFilter), "ig"),
                Criteria.where(TaskSkipErrorTableDto.FIELD_TARGET_TABLE).regex(String.format(".*%s.*", tableFilter), "ig")
            );
        }

        Query query = new Query(criteria);
        long count = mongoTemplate.count(query, entityClass);
        if (count == 0) {
            return Page.empty();
        }

        query.skip(skip).limit(limit).with(sort);
        List<TaskSkipErrorTableDto> dataList = mongoTemplate.find(query, entityClass).stream()
            .map(this::convert)
            .collect(Collectors.toList());

        return Page.page(dataList, count);
    }

    public List<SkipErrorTableStatusVo> getAllRecoverTableNames(String taskId) {
        Query query = Query.query(Criteria.where(TaskSkipErrorTableDto.FIELD_TASK_ID).is(taskId));
        query.fields().include(
            TaskSkipErrorTableDto.FIELD_STATUS,
            TaskSkipErrorTableDto.FIELD_SOURCE_TABLE
        );

        List<TaskSkipErrorTableEntity> entities = mongoTemplate.find(query, entityClass);
        return entities.stream()
            .map(e -> SkipErrorTableStatusVo.create()
                .sourceTable(e.getSourceTable())
                .status(e.getStatus())
            ).toList();
    }

    public long changeTableStatus(String taskId, List<String> sourceTables, SkipErrorTableStatusEnum status) {
        Criteria criteria = Criteria.where(TaskSkipErrorTableDto.FIELD_TASK_ID).is(taskId);
        if (null != sourceTables && !sourceTables.isEmpty()) {
            criteria.and(TaskSkipErrorTableDto.FIELD_SOURCE_TABLE).in(sourceTables);
        }
        Query query = Query.query(criteria);
        Update update = Update.update(TaskSkipErrorTableDto.FIELD_STATUS, status.name())
            .set(TaskSkipErrorTableDto.FIELD_UPDATED, new Date());

        mongoTemplate.updateFirst(query, update, entityClass);
        return 0;
    }
}
