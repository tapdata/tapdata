package com.tapdata.tm.metadatadefinition.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.metadatadefinition.entity.MetadataDefinitionEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Repository
public class MetadataDefinitionRepository extends BaseRepository<MetadataDefinitionEntity, ObjectId> {
    public MetadataDefinitionRepository(MongoTemplate mongoOperations) {
        super(MetadataDefinitionEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
