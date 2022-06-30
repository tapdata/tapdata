package com.tapdata.tm.scheduleTasks.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.scheduleTasks.entity.ScheduleTasksEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/10 下午4:30
 * @description
 */
@Repository
public class ScheduleTasksRepository extends BaseRepository<ScheduleTasksEntity, ObjectId> {
    public ScheduleTasksRepository(MongoTemplate mongoOperations) {
        super(ScheduleTasksEntity.class, mongoOperations);
    }
}
