package com.tapdata.tm.userGroup.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.userGroup.entity.UserGroupEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/12/01
 * @Description:
 */
@Repository
public class UserGroupRepository extends BaseRepository<UserGroupEntity, ObjectId> {
    public UserGroupRepository(MongoTemplate mongoOperations) {
        super(UserGroupEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
