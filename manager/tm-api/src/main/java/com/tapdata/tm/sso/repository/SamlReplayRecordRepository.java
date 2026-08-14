package com.tapdata.tm.sso.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.sso.entity.SamlReplayRecord;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * Repository for {@link SamlReplayRecord} one-time message records.
 */
@Repository
public class SamlReplayRecordRepository extends BaseRepository<SamlReplayRecord, ObjectId> {

    public SamlReplayRecordRepository(MongoTemplate mongoOperations) {
        super(SamlReplayRecord.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
