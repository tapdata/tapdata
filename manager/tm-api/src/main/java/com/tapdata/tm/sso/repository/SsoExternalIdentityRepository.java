package com.tapdata.tm.sso.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.sso.entity.SsoExternalIdentity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * Repository for {@link SsoExternalIdentity} bindings.
 */
@Repository
public class SsoExternalIdentityRepository extends BaseRepository<SsoExternalIdentity, ObjectId> {

    public SsoExternalIdentityRepository(MongoTemplate mongoOperations) {
        super(SsoExternalIdentity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
