package com.tapdata.tm.group.repostitory;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.group.entity.GroupInfoRecordEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class GroupInfoRecordRepository extends BaseRepository<GroupInfoRecordEntity, ObjectId> {

    public GroupInfoRecordRepository(MongoTemplate mongoOperations) {
        super(GroupInfoRecordEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
