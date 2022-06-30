package com.tapdata.tm.cluster.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.cluster.entity.ClusterStateEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/09/13
 * @Description:
 */
@Repository
public class ClusterStateRepository extends BaseRepository<ClusterStateEntity, ObjectId> {
    public ClusterStateRepository(MongoTemplate mongoOperations) {
        super(ClusterStateEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
