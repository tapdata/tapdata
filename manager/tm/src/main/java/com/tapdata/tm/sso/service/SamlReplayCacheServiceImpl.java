package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.entity.SamlReplayRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.util.Date;

/**
 * MongoDB-backed {@link SamlReplayCacheService}. Relies on the unique compound
 * index {@code (type, recordId)} (created by the startup patch) to make the first
 * insert win and any concurrent/subsequent insert of the same id fail with a
 * {@link DuplicateKeyException}, which we translate into "already consumed".
 */
@Service
public class SamlReplayCacheServiceImpl implements SamlReplayCacheService {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Override
    public boolean recordIfFirstUse(String type, String recordId) {
        if (StringUtils.isBlank(recordId)) {
            // No id to correlate on; cannot guarantee single use -> treat as replay.
            return false;
        }
        SamlReplayRecord record = new SamlReplayRecord();
        record.setType(StringUtils.defaultString(type));
        record.setRecordId(recordId);
        record.setCreatedAt(new Date());
        try {
            mongoTemplate.insert(record);
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }
}
