package com.tapdata.tm.job.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.job.entity.JobEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/09/13
 * @Description:
 */
@Repository
public class JobRepository extends BaseRepository<JobEntity, ObjectId> {
    public JobRepository(MongoTemplate mongoOperations) {
        super(JobEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
