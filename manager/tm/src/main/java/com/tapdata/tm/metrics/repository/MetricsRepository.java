package com.tapdata.tm.metrics.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.metrics.entity.MetricsEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/9 下午4:47
 */
@Repository
public class MetricsRepository extends BaseRepository<MetricsEntity, ObjectId> {
    public MetricsRepository(MongoTemplate mongoOperations) {
        super(MetricsEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
