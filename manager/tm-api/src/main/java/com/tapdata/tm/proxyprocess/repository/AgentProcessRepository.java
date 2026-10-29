package com.tapdata.tm.proxyprocess.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.proxyprocess.entity.AgentProcessEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class AgentProcessRepository extends BaseRepository<AgentProcessEntity, ObjectId> {
    public AgentProcessRepository(MongoTemplate mongoOperations) {
        super(AgentProcessEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }

    public Optional<AgentProcessEntity> findByAgentId(String agentId) {
        return findOne(Query.query(Criteria.where("agentId").is(agentId)));
    }

    public Optional<AgentProcessEntity> findById(ObjectId id) {
        return findOne(Query.query(Criteria.where("_id").is(id)));
    }

    public Optional<AgentProcessEntity> findByToken(String token) {
        return findOne(Query.query(Criteria.where("token").is(token)));
    }

    public AgentProcessEntity saveAgentProcess(AgentProcessEntity entity) {
        return mongoOperations.save(entity, entityInformation.getCollectionName());
    }
}
