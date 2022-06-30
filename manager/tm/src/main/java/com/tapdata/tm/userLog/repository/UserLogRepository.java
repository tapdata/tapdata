package com.tapdata.tm.userLog.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.userLog.entity.UserLogs;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class UserLogRepository extends BaseRepository<UserLogs, ObjectId> {

    public UserLogRepository(MongoTemplate mongoOperations) {
        super(UserLogs.class, mongoOperations);
    }
}
