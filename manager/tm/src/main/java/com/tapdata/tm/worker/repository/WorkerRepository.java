package com.tapdata.tm.worker.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.worker.entity.Worker;
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

        if (entity.getPingTime() == null || entity.getPingTime() == 0) {
            entity.setPingTime(new Date().getTime());
        }
        return super.upsert(query, entity);
    }
}
