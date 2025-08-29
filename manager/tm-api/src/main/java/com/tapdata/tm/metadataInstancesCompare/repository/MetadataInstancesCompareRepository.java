package com.tapdata.tm.metadataInstancesCompare.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;

import com.tapdata.tm.metadataInstancesCompare.entity.MetadataInstancesCompareEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class MetadataInstancesCompareRepository extends BaseRepository<MetadataInstancesCompareEntity, ObjectId> {
    public MetadataInstancesCompareRepository(MongoTemplate mongoOperations) {
        super(MetadataInstancesCompareEntity.class, mongoOperations);
    }
}
