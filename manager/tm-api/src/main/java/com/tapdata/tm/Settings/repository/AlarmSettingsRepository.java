package com.tapdata.tm.Settings.repository;

import com.tapdata.tm.Settings.entity.AlarmSetting;
import com.tapdata.tm.base.reporitory.BaseRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;


@Repository
public class AlarmSettingsRepository  extends BaseRepository<AlarmSetting, ObjectId> {
     public AlarmSettingsRepository(MongoTemplate mongoOperations) {
          super(AlarmSetting.class, mongoOperations);
     }
}
