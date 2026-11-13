package com.tapdata.tm.proxyprocess.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.proxyprocess.constant.AgentProcessType;
import com.tapdata.tm.proxyprocess.entity.AgentProcessEntity;
import com.tapdata.tm.proxyprocess.entity.OracleLogParserAgentProcessEntity;
import org.springframework.dao.DuplicateKeyException;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Repository
public class AgentProcessRepository extends BaseRepository<AgentProcessEntity, ObjectId> {
    private final MongoTemplate mongoTemplate;

    public AgentProcessRepository(MongoTemplate mongoOperations) {
        super(AgentProcessEntity.class, mongoOperations);
        this.mongoTemplate = mongoOperations;
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

    public List<AgentProcessEntity> findOracleLogParserProjections(Query query) {
        query.addCriteria(Criteria.where("sourceServiceId").exists(true).nin(null, ""));
        return findAll(query);
    }

    public List<String> findAgentTypes() {
        Query query = Query.query(Criteria.where("agentType").exists(true).nin(null, ""));
        return mongoTemplate.findDistinct(query, "agentType", AgentProcessEntity.class, String.class)
                .stream()
                .filter(org.apache.commons.lang3.StringUtils::isNotBlank)
                .sorted(String.CASE_INSENSITIVE_ORDER.thenComparing(Comparator.naturalOrder()))
                .toList();
    }

    public boolean upsertOracleLogParser(OracleLogParserAgentProcessEntity entity) {
        Criteria latestReport = new Criteria().orOperator(
                Criteria.where("statusReportTime").exists(false),
                Criteria.where("statusReportTime").is(null),
                Criteria.where("statusReportTime").lte(entity.getStatusReportTime())
        );
        Query query = Query.query(new Criteria().andOperator(
                Criteria.where("agentId").is(entity.getAgentId()),
                Criteria.where("agentType").is(AgentProcessType.ORACLE_LOG_PARSER.getValue()),
                latestReport
        ));

        Update update = new Update()
                .set("agentName", entity.getAgentName())
                .set("agentType", entity.getAgentType())
                .set("advertiseIp", entity.getAdvertiseIp())
                .set("advertisePort", entity.getAdvertisePort())
                .set("startTime", entity.getStartTime())
                .set("status", entity.getStatus())
                .set("statusReportTime", entity.getStatusReportTime())
                .set("pid", entity.getPid())
                .set("cpuUsage", entity.getCpuUsage())
                .set("memoryUsage", entity.getMemoryUsage())
                .set("errorReason", entity.getErrorReason())
                .set("lastRegisterTime", entity.getLastRegisterTime())
                .set("token", "")
                .set("sourceServiceId", entity.getSourceServiceId())
                .set("sourceState", entity.getSourceState())
                .set("_class", "oracleLogParserAgentProcess")
                .setOnInsert("agentId", entity.getAgentId());

        try {
            var result = mongoTemplate.upsert(query, update, OracleLogParserAgentProcessEntity.class);
            return result.getMatchedCount() > 0 || result.getUpsertedId() != null;
        } catch (DuplicateKeyException duplicate) {
            // A newer report may have won a concurrent upsert; the agentId unique index
            // keeps this projection idempotent and prevents stale events from replacing it.
            findByAgentId(entity.getAgentId())
                    .filter(existing -> !AgentProcessType.ORACLE_LOG_PARSER.getValue()
                            .equals(existing.getAgentType()))
                    .ifPresent(existing -> org.slf4j.LoggerFactory
                            .getLogger(AgentProcessRepository.class)
                            .warn("Skip OracleLogParser projection because agentId {} belongs to type {}",
                                    entity.getAgentId(), existing.getAgentType()));
            return false;
        }
    }

    public void deleteOracleLogParser(String agentId) {
        mongoTemplate.remove(Query.query(Criteria.where("agentId").is(agentId)
                .and("agentType").is(AgentProcessType.ORACLE_LOG_PARSER.getValue())), AgentProcessEntity.class);
    }
}
