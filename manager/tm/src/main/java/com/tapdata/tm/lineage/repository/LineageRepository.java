package com.tapdata.tm.lineage.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.lineage.entity.LineageEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2023/05/19
 * @Description:
 */
@Repository
public class LineageRepository extends BaseRepository<LineageEntity, ObjectId> {
    public LineageRepository(MongoTemplate mongoOperations) {
        super(LineageEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
