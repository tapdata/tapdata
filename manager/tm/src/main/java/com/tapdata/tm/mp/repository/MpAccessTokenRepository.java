package com.tapdata.tm.mp.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.mp.entity.MpAccessToken;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2023/1/17 下午4:19
 */
@Repository
public class MpAccessTokenRepository extends BaseRepository<MpAccessToken, ObjectId> {
    public MpAccessTokenRepository(MongoTemplate mongoOperations) {
        super(MpAccessToken.class, mongoOperations);
    }
}
