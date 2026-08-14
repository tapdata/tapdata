package com.tapdata.tm.sso.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.sso.entity.SsoSession;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * Repository for {@link SsoSession} active-session records.
 */
@Repository
public class SsoSessionRepository extends BaseRepository<SsoSession, ObjectId> {

    public SsoSessionRepository(MongoTemplate mongoOperations) {
        super(SsoSession.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
