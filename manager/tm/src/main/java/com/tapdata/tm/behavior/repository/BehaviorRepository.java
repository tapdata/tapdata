package com.tapdata.tm.behavior.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.behavior.entity.BehaviorEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/6/21 下午4:17
 */
@Repository
public class BehaviorRepository extends BaseRepository<BehaviorEntity, ObjectId> {
    public BehaviorRepository(MongoTemplate mongoOperations) {
        super(BehaviorEntity.class, mongoOperations);
    }
}
