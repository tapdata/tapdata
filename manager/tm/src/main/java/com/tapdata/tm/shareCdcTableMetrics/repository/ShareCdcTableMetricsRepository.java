package com.tapdata.tm.shareCdcTableMetrics.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.shareCdcTableMetrics.entity.ShareCdcTableMetricsEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2023/03/09
 * @Description:
 */
@Repository
public class ShareCdcTableMetricsRepository extends BaseRepository<ShareCdcTableMetricsEntity, ObjectId> {
    public ShareCdcTableMetricsRepository(MongoTemplate mongoOperations) {
        super(ShareCdcTableMetricsEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
