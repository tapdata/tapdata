package com.tapdata.tm.clusterOperation.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.clusterOperation.entity.ClusterOperationEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/09/13
 * @Description:
 */
@Repository
public class ClusterOperationRepository extends BaseRepository<ClusterOperationEntity, ObjectId> {
    public ClusterOperationRepository(MongoTemplate mongoOperations) {
        super(ClusterOperationEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
