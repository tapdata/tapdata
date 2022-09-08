package com.tapdata.tm.monitor.service;

import com.tapdata.tm.monitor.entity.MeasureLockEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.Date;


@Slf4j
@Service
public class MeasureLockService {
    @Autowired
    private MongoTemplate mongoOperations;

    public boolean lock(String granularity, Date date, String unique) {
        Query query = Query.query(Criteria.where("granularity").is(granularity).and("time").is(date));

        Update update = new Update();
        update.setOnInsert("time", date);
        update.setOnInsert("unique", unique);
        update.setOnInsert("createdTime", new Date());
        MeasureLockEntity measureLockEntity = mongoOperations.findAndModify(query, update, FindAndModifyOptions.options().returnNew(true).upsert(true), MeasureLockEntity.class);
        if (null == measureLockEntity) {
            return true;
        }
        return StringUtils.equals(measureLockEntity.getUnique(), unique);
    }

    public void unlock(String granularity, Date date, String unique) {
        Query query = Query.query(Criteria.where("granularity").is(granularity).and("time").is(date).and("unique").is(unique));
        mongoOperations.findAndRemove(query, MeasureLockEntity.class);
    }
}
