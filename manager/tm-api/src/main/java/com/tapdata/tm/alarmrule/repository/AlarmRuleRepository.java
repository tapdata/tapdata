package com.tapdata.tm.alarmrule.repository;

import com.tapdata.tm.alarmrule.entity.AlarmRule;
import com.tapdata.tm.base.reporitory.BaseRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;


@Repository
public class AlarmRuleRepository extends BaseRepository<AlarmRule, ObjectId> {
     public AlarmRuleRepository(MongoTemplate mongoOperations) {
          super(AlarmRule.class, mongoOperations);
     }
}
