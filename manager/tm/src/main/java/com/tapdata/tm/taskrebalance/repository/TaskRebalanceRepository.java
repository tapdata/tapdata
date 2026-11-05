package com.tapdata.tm.taskrebalance.repository;

import com.mongodb.DuplicateKeyException;
import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.taskrebalance.dto.TaskRebalanceDto;
import com.tapdata.tm.taskrebalance.entity.TaskRebalanceEntity;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.Optional;

@Repository
public class TaskRebalanceRepository extends BaseRepository<TaskRebalanceEntity, ObjectId> {

    public TaskRebalanceRepository(MongoTemplate mongoOperations) {
        super(TaskRebalanceEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
        createIndex(new BsonDocument(TaskRebalanceDto.FIELD_STATUS, new BsonInt32(1)),
                new IndexOptions().background(true));
        createIndex(new BsonDocument(TaskRebalanceDto.FIELD_CREATE_TIME, new BsonInt32(-1)),
                new IndexOptions().background(true));
        createIndex(new BsonDocument(TaskRebalanceDto.FIELD_IS_ACTIVED, new BsonInt32(1)),
                new IndexOptions().background(true).sparse(true).unique(true));
    }

    public Optional<TaskRebalanceEntity> acquireActiveCreating(TaskRebalanceEntity creating, UserDetail userDetail) {
        Date now = new Date();
        Query query = Query.query(Criteria.where(TaskRebalanceDto.FIELD_IS_ACTIVED).is(true));
        Update update = new Update()
                .setOnInsert("_id", creating.getId())
                .setOnInsert(TaskRebalanceDto.FIELD_NAME, creating.getName())
                .setOnInsert(TaskRebalanceDto.FIELD_STATUS, creating.getStatus())
                .setOnInsert(TaskRebalanceDto.FIELD_IS_ACTIVED, true)
                .setOnInsert(TaskRebalanceDto.FIELD_TOTAL_COUNT, creating.getTotalCount())
                .setOnInsert(TaskRebalanceDto.FIELD_PENDING_COUNT, creating.getPendingCount())
                .setOnInsert(TaskRebalanceDto.FIELD_STOPPING_COUNT, creating.getStoppingCount())
                .setOnInsert(TaskRebalanceDto.FIELD_STARTING_COUNT, creating.getStartingCount())
                .setOnInsert(TaskRebalanceDto.FIELD_OK_COUNT, creating.getOkCount())
                .setOnInsert(TaskRebalanceDto.FIELD_FAILED_COUNT, creating.getFailedCount())
                .setOnInsert(TaskRebalanceDto.FIELD_CANCELLED_COUNT, creating.getCancelledCount())
                .setOnInsert(TaskRebalanceDto.FIELD_CREATE_TIME, now)
                .setOnInsert("customId", userDetail.getCustomerId())
                .setOnInsert(TaskRebalanceDto.FIELD_USER_ID, userDetail.getUserId())
                .setOnInsert("createUser", userDetail.getUsername())
                .set("last_updated", now)
                .set("lastUpdBy", userDetail.getUserId());
        try {
            return Optional.ofNullable(mongoOperations.findAndModify(
                    query,
                    update,
                    FindAndModifyOptions.options().returnNew(true).upsert(true),
                    TaskRebalanceEntity.class
            ));
        } catch (DuplicateKeyException e) {
            return findOne(query);
        }
    }

    @Override
    public Query applyUserDetail(Query query, UserDetail userDetail) {
        removeFilter(TaskRebalanceDto.FIELD_CUSTOM_ID, query);
        removeFilter(TaskRebalanceDto.FIELD_USER_ID, query);
        return query;
    }
}
