package com.tapdata.tm.shareCdcTableMapping.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.shareCdcTableMapping.entity.ShareCdcTableMappingEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2023/10/16
 * @Description:
 */
@Repository
public class ShareCdcTableMappingRepository extends BaseRepository<ShareCdcTableMappingEntity, ObjectId> {
    public ShareCdcTableMappingRepository(MongoTemplate mongoOperations) {
        super(ShareCdcTableMappingEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
