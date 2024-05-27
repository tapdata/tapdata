package com.tapdata.tm.featurecheck.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.dictionary.entity.DictionaryEntity;
import com.tapdata.tm.featurecheck.entity.FeatureCheckEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class FeatureCheckRepository extends BaseRepository<FeatureCheckEntity, ObjectId> {
    public FeatureCheckRepository(MongoTemplate mongoOperations) {
        super(FeatureCheckEntity.class, mongoOperations);
    }
}
