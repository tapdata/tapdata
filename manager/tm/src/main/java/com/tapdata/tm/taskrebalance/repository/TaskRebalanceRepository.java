package com.tapdata.tm.taskrebalance.repository;

import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.taskrebalance.dto.TaskRebalanceDto;
import com.tapdata.tm.taskrebalance.entity.TaskRebalanceEntity;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

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
    }

    @Override
    public Query applyUserDetail(Query query, UserDetail userDetail) {
        removeFilter(TaskRebalanceDto.FIELD_CUSTOM_ID, query);
        removeFilter(TaskRebalanceDto.FIELD_USER_ID, query);
        return query;
    }
}
