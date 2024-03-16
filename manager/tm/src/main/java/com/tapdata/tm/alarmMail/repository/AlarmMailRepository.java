package com.tapdata.tm.alarmMail.repository;

import com.tapdata.tm.alarmMail.entity.AlarmMail;
import com.tapdata.tm.base.reporitory.BaseRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class AlarmMailRepository extends BaseRepository<AlarmMail, ObjectId> {
    public AlarmMailRepository(MongoTemplate mongoOperations) {
        super(AlarmMail.class, mongoOperations);
    }
}
