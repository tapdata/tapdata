package com.tapdata.tm.group.repostitory;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.group.entity.GroupInfoEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class GroupInfoRepository extends BaseRepository<GroupInfoEntity, ObjectId> {

    public GroupInfoRepository(MongoTemplate mongoOperations) {
        super(GroupInfoEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
