package com.tapdata.tm.taskrebalance.repository;

import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.taskrebalance.dto.TaskRebalanceJobDto;
import com.tapdata.tm.taskrebalance.entity.TaskRebalanceJobEntity;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

@Repository
public class TaskRebalanceJobRepository extends BaseRepository<TaskRebalanceJobEntity, ObjectId> {

    public TaskRebalanceJobRepository(MongoTemplate mongoOperations) {
        super(TaskRebalanceJobEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
        createIndex(new BsonDocument(TaskRebalanceJobDto.FIELD_REBALANCE_ID, new BsonInt32(1))
                        .append(TaskRebalanceJobDto.FIELD_STATUS, new BsonInt32(1)),
                new IndexOptions().background(true));
        createIndex(new BsonDocument(TaskRebalanceJobDto.FIELD_TASK_ID, new BsonInt32(1))
                        .append(TaskRebalanceJobDto.FIELD_STATUS, new BsonInt32(1)),
                new IndexOptions().background(true));
        createIndex(new BsonDocument(TaskRebalanceJobDto.FIELD_REBALANCE_ID, new BsonInt32(1))
                        .append(TaskRebalanceJobDto.FIELD_CREATE_TIME, new BsonInt32(1)),
                new IndexOptions().background(true));
        createIndex(new BsonDocument(TaskRebalanceJobDto.FIELD_REBALANCE_ID, new BsonInt32(1))
                        .append(TaskRebalanceJobDto.FIELD_TASK_ID, new BsonInt32(1)),
                new IndexOptions().background(true).unique(true));
    }

    @Override
    public Query applyUserDetail(Query query, UserDetail userDetail) {
        removeFilter(TaskRebalanceJobDto.FIELD_CUSTOM_ID, query);
        removeFilter(TaskRebalanceJobDto.FIELD_USER_ID, query);
        return query;
    }
}
