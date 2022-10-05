package com.tapdata.tm.monitor.service;

import com.tapdata.tm.monitor.entity.MeasureLockEntity;
import com.tapdata.tm.utils.TimeUtil;
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

    /**
     * TM每小时竞争锁, 多个实例同时尝试写入tm的进程id,只有一个实例可以写入成功,并返回被写入的doc,这个实例就是获得执行权的实例
     * setOnInsert 操作符会将指定的值赋值给指定的字段，如果要更新的文档存在那么$setOnInsert操作符不做任何处理；
     *
     * @return
     */
    @Deprecated
    public MeasureLockEntity tryGetLock(String tmProcessName) {
        Date now = new Date();
        Date hour = TimeUtil.cleanTimeAfterHour(now);
        Query query = Query.query(Criteria.where("hour").is(hour).and("tmProcessName").is(tmProcessName));

        Update update = new Update();
        update.setOnInsert("tmProcessName", tmProcessName );
        update.set("createdTime",new Date());
        MeasureLockEntity measureLockEntity = mongoOperations.findAndModify(query, update, FindAndModifyOptions.options().returnNew(true).upsert(true), MeasureLockEntity.class);
        return measureLockEntity;
    }

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
