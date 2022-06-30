package com.tapdata.tm.jobddlhistories.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.jobddlhistories.entity.JobDDLHistoriesEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/09/13
 * @Description:
 */
@Repository
public class JobDDLHistoriesRepository extends BaseRepository<JobDDLHistoriesEntity, ObjectId> {
    public JobDDLHistoriesRepository(MongoTemplate mongoOperations) {
        super(JobDDLHistoriesEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
