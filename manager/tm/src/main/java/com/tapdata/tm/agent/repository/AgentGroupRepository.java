package com.tapdata.tm.agent.repository;

import com.tapdata.tm.agent.entity.AgentGroupEntity;
import com.tapdata.tm.base.reporitory.BaseRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author: Gavin
 * @Date: 2023/03/27
 * @Description:
 */
@Repository
public class AgentGroupRepository extends BaseRepository<AgentGroupEntity, ObjectId> {
    public AgentGroupRepository(MongoTemplate mongoOperations) {
        super(AgentGroupEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}