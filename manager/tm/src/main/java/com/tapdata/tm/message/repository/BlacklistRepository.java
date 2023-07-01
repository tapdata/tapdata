package com.tapdata.tm.message.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.message.entity.BlacklistEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2023/7/1 17:14
 */
@Repository
public class BlacklistRepository extends BaseRepository<BlacklistEntity, ObjectId> {
    public BlacklistRepository(MongoTemplate mongoOperations) {
        super(BlacklistEntity.class, mongoOperations);
    }
}
