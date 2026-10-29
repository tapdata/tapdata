package com.tapdata.tm.worker.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.worker.entity.Worker;
import org.springframework.dao.DuplicateKeyException;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.Date;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/10 下午4:30
 * @description
 */
@Repository
public class WorkerRepository extends BaseRepository<Worker, ObjectId> {
    public WorkerRepository(MongoTemplate mongoOperations) {
        super(Worker.class, mongoOperations);
    }

    @Override
    public long upsert(Query query, Worker entity) {
        prepareUpsert(entity);
        return super.upsert(query, entity);
    }

    @Override
    public long upsert(Query query, Worker entity, UserDetail userDetail) {
        prepareUpsert(entity);
        try {
            return super.upsert(query, entity, userDetail);
        } catch (DuplicateKeyException exception) {
            // A concurrent API Server heartbeat may lose the unique-key race. Retrying the
            // same upsert makes the loser converge on the record created by the winner.
            return super.upsert(query, entity, userDetail);
        }
    }

    private void prepareUpsert(Worker entity) {
        if (entity.getPingTime() == null || entity.getPingTime() == 0) {
            entity.setPingTime(new Date().getTime());
        }
        if ("api-server".equals(entity.getWorkerType()) && entity.getDelete() == null) {
            entity.setDelete(false);
        }
    }
}
