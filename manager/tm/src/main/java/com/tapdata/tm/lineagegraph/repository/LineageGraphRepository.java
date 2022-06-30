package com.tapdata.tm.lineagegraph.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.lineagegraph.entity.LineageGraphEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Repository
public class LineageGraphRepository extends BaseRepository<LineageGraphEntity, ObjectId> {
    public LineageGraphRepository(MongoTemplate mongoOperations) {
        super(LineageGraphEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
