package com.tapdata.tm.task.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.annotation.SetOnInsert;
import com.tapdata.manager.common.utils.ReflectionUtils;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.entity.ProjectEntity;
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
import java.util.Optional;

/**
 * @Author:
 * @Date:
 * @Description: Project repository for handling database operations
 */
@Repository
public class ProjectRepository extends BaseRepository<ProjectEntity, ObjectId> {
    public ProjectRepository(MongoTemplate mongoOperations) {
        super(ProjectEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }

    public ProjectEntity importEntity(ProjectEntity entity, UserDetail userDetail) {
        Assert.notNull(entity, "Entity must not be null!");

        applyUserDetail(entity, userDetail);
        beforeCreateEntity(entity, userDetail);
        Query id = new Query(Criteria.where("_id").is(entity.getId()));
        upsert(id, entity);
        Optional<ProjectEntity> one = findOne(id, userDetail);
        return one.orElse(null);
    }

    @Override
    public Update buildUpdateSet(ProjectEntity entity) {
        return buildUpdateSet(entity, null);
    }

    @Override
    public Update buildUpdateSet(ProjectEntity entity, UserDetail userDetail) {
        Update update = new Update();
        Field[] fields = ReflectionUtils.getAllDeclaredFields(ProjectEntity.class);

        for (Field field : fields) {
            if ("$jacocoData".equals(field.getName())) {
                continue;
            }
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
    public Update buildReplaceSet(ProjectEntity entity) {
        Update update = new Update();
        Field[] fields = ReflectionUtils.getAllDeclaredFields(ProjectEntity.class);
        for (Field field : fields) {
            if (field.getName().equals("id")) {
                continue;
            }
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

        return mongoOperations.updateFirst(query, update, ProjectEntity.class);
    }

    public UpdateResult update(Query query, Update update, UserDetail userDetail) {
        Assert.notNull(update, "Update must not be null!");
        update = filterStatus(update);
        applyUserDetail(query, userDetail);

        return mongoOperations.updateMulti(query, update, ProjectEntity.class);
    }

    public ProjectEntity findAndModify(Query query, Update update, UserDetail userDetail) {
        Assert.notNull(update, "Update must not be null!");
        update = filterStatus(update);
        return findAndModify(query, update, null, userDetail);
    }

    public ProjectEntity findAndModify(Query query, Update update, FindAndModifyOptions options, UserDetail userDetail) {
        Assert.notNull(update, "Update must not be null!");
        update = filterStatus(update);
        if (options == null) {
            options = new FindAndModifyOptions();
            options.returnNew(true);
        }

        applyUserDetail(query, userDetail);
        beforeUpsert(update, userDetail);

        return mongoOperations.findAndModify(query, update, options, ProjectEntity.class);
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

    public Update deleteUpdate(Update update) {
        update.set("isDeleted", true);
        return update;
    }

} 