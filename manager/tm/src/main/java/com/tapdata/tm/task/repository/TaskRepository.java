package com.tapdata.tm.task.repository;

import com.fasterxml.jackson.databind.ser.std.ObjectArraySerializer;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.annotation.SetOnInsert;
import com.tapdata.manager.common.utils.ReflectionUtils;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.utils.Lists;
import org.apache.commons.lang3.ObjectUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

/**
 * @Author:
 * @Date: 2021/11/03
 * @Description:
 */
@Repository
public class TaskRepository extends BaseRepository<TaskEntity, ObjectId> {
    public TaskRepository(MongoTemplate mongoOperations) {
        super(TaskEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }

    public TaskEntity importEntity(TaskEntity entity, UserDetail userDetail) {
        Assert.notNull(entity, "Entity must not be null!");

        applyUserDetail(entity, userDetail);
        beforeCreateEntity(entity, userDetail);
        Query id = new Query(Criteria.where("_id").is(entity.getId()));
        upsert(id, entity);
        Optional<TaskEntity> one = findOne(id, userDetail);
        return one.orElse(null);
    }

    @Override
    public Update buildUpdateSet(TaskEntity entity) {
        return buildUpdateSet(entity, null);
    }

    @Override
    public Update buildUpdateSet(TaskEntity entity, UserDetail userDetail) {
        Update update = new Update();
        Field[] files = ReflectionUtils.getAllDeclaredFields(TaskEntity.class);
//        if (userDetail != null){
//            applyUserDetail(entity, userDetail);
//        }

        for (Field field : files) {
            if ("$jacocoData".equals(field.getName())) {
                continue;
            }
            //控制其他的方法修改任务状态。状态流转需要同意控制
            if ("status".equals(field.getName())) {
                continue;
            }
            Object value = ReflectionUtils.getField(field, entity);
            if (value != null) {
                SetOnInsert setOnInsert = field.getAnnotation(SetOnInsert.class);
                if (setOnInsert != null) {
                    update.setOnInsert(field.getName(), value);
                } else {
                    update.set(field.getName(), value);
                }
            }
        }
        return update;
    }


    @Override
    public Update buildReplaceSet(TaskEntity entity) {
        Update update = new Update();
        Field[] files = ReflectionUtils.getAllDeclaredFields(TaskEntity.class);
        for (Field field : files) {
            if (field.getName().equals("id")) {
                continue;
            }
            //控制其他的方法修改任务状态。状态流转需要同意控制
            if (field.getName().equals("status")) {
                continue;
            }
            Object value = ReflectionUtils.getField(field, entity);
            update.set(field.getName(), value);
        }
        return update;
    }

    public UpdateResult updateFirst(Query query, Update update, UserDetail userDetail) {
        Assert.notNull(query, "Query must not be null!");

        Assert.notNull(update, "Update must not be null!");
        update = filterStatus(update);

        applyUserDetail(query, userDetail);
        update.set("lastUpdAt", new Date());
        update.set("lastUpdBy", userDetail.getUserId());

        update.getUpdateObject().forEach((k,v) -> {
            if ("$unset".equals(k) && v instanceof Document) {
                Document setUpdate = (Document) v;
                if (setUpdate.containsKey("agentId") && ObjectUtils.isEmpty(setUpdate.get("agentId"))) {
                    setUpdate.remove("agentId");
                }
                if (setUpdate.isEmpty()) {
                    setUpdate.put("agentIdTemp", 1);
                }
            } else if ("$set".equals(k) && v instanceof Document) {
                Document setUpdate = (Document) v;
                if (setUpdate.containsKey("agentId") && ObjectUtils.isEmpty(setUpdate.get("agentId"))) {
                    setUpdate.remove("agentId");
                }
                if (setUpdate.isEmpty()) {
                    setUpdate.put("agentIdTemp", 1);
                }
            }
        });

        return mongoOperations.updateFirst(query, update, TaskEntity.class);
    }

    public UpdateResult update(Query query, Update update, UserDetail userDetail) {
        Assert.notNull(update, "Update must not be null!");
        update = filterStatus(update);
        applyUserDetail(query, userDetail);

        return mongoOperations.updateMulti(query, update, TaskEntity.class);
    }


    public UpdateResult update(Query query, Update update) {
        Assert.notNull(update, "Update must not be null!");
        update = filterStatus(update);
        return mongoOperations.updateFirst(query, update, TaskEntity.class);
    }


    public TaskEntity findAndModify(Query query, Update update, UserDetail userDetail) {
        Assert.notNull(update, "Update must not be null!");
        update = filterStatus(update);
        return findAndModify(query, update, null, userDetail);
    }

    public TaskEntity findAndModify(Query query, Update update, FindAndModifyOptions options, UserDetail userDetail) {
        Assert.notNull(update, "Update must not be null!");
        update = filterStatus(update);
        if (options == null) {
            options = new FindAndModifyOptions();
            options.returnNew(true);
        }

        applyUserDetail(query, userDetail);
        beforeUpsert(update, userDetail);

        TaskEntity entity = mongoOperations.findAndModify(query, update, options, TaskEntity.class);

        return entity;
    }

    private Update filterStatus(Update update) {
        if (update.modifies("status")) {
            Document updateObject = update.getUpdateObject();
            updateObject.forEach((k, v) -> {
                Document v1 = (Document) v;
                v1.remove("status");
            });
            update = Update.fromDocument(updateObject);

        }
        return update;
    }
}
