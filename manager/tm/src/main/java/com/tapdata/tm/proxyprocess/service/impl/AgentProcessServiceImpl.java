package com.tapdata.tm.proxyprocess.service.impl;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.proxyprocess.constant.AgentProcessStatus;
import com.tapdata.tm.proxyprocess.constant.AgentProcessType;
import com.tapdata.tm.proxyprocess.dto.AgentProcessDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessQueryDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterResponseDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessStatusDto;
import com.tapdata.tm.proxyprocess.dto.OracleLogParserAgentProcessDto;
import com.tapdata.tm.proxyprocess.entity.AgentProcessEntity;
import com.tapdata.tm.proxyprocess.entity.OracleLogParserAgentProcessEntity;
import com.tapdata.tm.proxyprocess.repository.AgentProcessRepository;
import com.tapdata.tm.proxyprocess.service.AgentProcessService;
import com.tapdata.tm.proxyprocess.service.auth.AgentCredentialPolicy;
import com.tapdata.tm.proxyprocess.service.auth.AgentCredentialService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
public class AgentProcessServiceImpl implements AgentProcessService {
    static final long OFFLINE_THRESHOLD_MILLIS = 5 * 60 * 1000L;

    private final AgentProcessRepository repository;
    /**
     * The standard credential chain shared by every proxied agent type. Null in
     * the repository-only form used by tests that never exercise credentials.
     */
    private final AgentCredentialService agentCredentialService;

    /**
     * Repository-only form used by code and tests that never touch the credential chain.
     *
     * <p>The access-code helpers are nullable: a caller that only exercises registration and
     * status without credentials (as the older tests do) keeps working, while the reader
     * credential path is skipped rather than failing with a null pointer.
     */
    public AgentProcessServiceImpl(AgentProcessRepository repository) {
        this(repository, null);
    }

    @Autowired
    public AgentProcessServiceImpl(AgentProcessRepository repository,
                                   AgentCredentialService agentCredentialService) {
        this.repository = repository;
        this.agentCredentialService = agentCredentialService;
    }

    @Override
    public AgentProcessRegisterResponseDto register(AgentProcessRegisterDto request, UserDetail user) {
        AgentProcessEntity entity;
        try {
            validateRegisterRequest(request);
            Optional<AgentProcessEntity> existingOptional = repository.findByAgentId(request.getAgentId());
            entity = existingOptional.orElseGet(AgentProcessEntity::new);
            boolean sameProcess = sameStartTime(entity.getStartTime(), request.getStartTime());
            /*
             * The registration belongs to the caller the endpoint authenticated. Re-registering is
             * routine - a Reader registers on every start - so this only records the owner.
             */
            entity.setUserId(user.getUserId());

            entity.setAgentName(request.getAgentName());
            entity.setAgentType(request.getAgentType());
            entity.setAgentId(request.getAgentId());
            entity.setAdvertiseIp(request.getAdvertiseIp());
            entity.setAdvertisePort(request.getAdvertisePort());
            entity.setStartTime(request.getStartTime().getTime());
            entity.setLastRegisterTime(System.currentTimeMillis());
            if (!sameProcess) {
                entity.setStatus(null);
                entity.setStatusReportTime(null);
                entity.setPid(null);
                entity.setCpuUsage(null);
                entity.setMemoryUsage(null);
                entity.setErrorReason(null);
            }
            applyReaderCredentials(request, entity);
        } catch (RuntimeException refused) {
            /*
             * A refused registration is recorded rather than only answered: the listing then
             * says why this Reader is not running instead of showing it as merely offline.
             * Written as a field update, not a save: the registration may have been written by
             * another request in the meantime and must not be rolled back to this copy.
             */
            try {
                repository.update(
                        Query.query(Criteria.where("agentId").is(request.getAgentId())
                                .and("agentType").is(request.getAgentType())),
                        new Update().set("status", AgentProcessStatus.FAILED)
                                .set("errorReason", refused.getMessage()));
            } catch (RuntimeException ignored) {
                /* The refusal is the answer either way; failing to note it must not mask it. */
            }
            throw refused;
        }
        repository.saveAgentProcess(entity);
        AgentProcessRegisterResponseDto response = new AgentProcessRegisterResponseDto();
        response.setProxyProcessId(entity.getProxyProcessId());
        response.setAgentId(entity.getAgentId());
        return response;
    }

    /**
     * Record the OAuth2 client credentials a Reader generated for itself.
     *
     * <p>The Reader owns these; TM only stores them. Two invariants matter:
     * <ul>
     *   <li>A client id already bound to another agent cannot be rebound, otherwise a caller who
     *       learns an agent id could redirect that Reader's traffic by re-registering.</li>
     *   <li>The secret is stored hashed; the plaintext only ever leaves through the explicit
     *       reveal endpoint, which an authorised caller uses when configuring a connection.</li>
     * </ul>
     * A Reader that upgrades keeps whatever it had before if it reports nothing new, so the
     * migration window does not wipe credentials for a half-rolled-out fleet.
     */
    private void applyReaderCredentials(AgentProcessRegisterDto request, AgentProcessEntity entity) {
        String clientId = request.getClientId();
        String clientSecret = request.getClientSecret();
        boolean reportsCredentials = StringUtils.isNotBlank(clientId)
                || StringUtils.isNotBlank(clientSecret);
        if (!reportsCredentials) {
            return;
        }
        if (StringUtils.isBlank(clientId) || StringUtils.isBlank(clientSecret)) {
            throw new IllegalArgumentException(
                    "Register failed, clientId and clientSecret must be reported together");
        }
        if (!AgentCredentialPolicy.isValidSecret(clientSecret)) {
            throw new IllegalArgumentException("Register failed, client secret is invalid");
        }
        /*
         * A client id belongs to one Reader. Checking it against the stored value would refuse
         * every restart, because the Reader reports the same client id it generated for itself,
         * so the question is whether some other registration of this type already uses it.
         */
        if (repository.clientIdUsedByAnotherAgent(request.getAgentType(), clientId,
                request.getAgentId())) {
            throw new IllegalArgumentException(
                    "Register failed, clientId is already used by another agent");
        }
        /*
         * Recorded through the shared credential chain, which keeps a hash for
         * change detection, an encrypted copy for the reveal endpoint, and
         * publishes the client when the secret is new.  Skipped in the
         * repository-only form used by tests that never exercise credentials.
         */
        if (agentCredentialService == null) {
            entity.setClientId(clientId);
            return;
        }
        agentCredentialService.acceptAgentCredentials(entity, clientId, clientSecret, null);
    }


    @Override
    public void reportStatus(AgentProcessStatusDto request) {
        validateStatusRequest(request);
        AgentProcessEntity entity = resolveReportingEntity(request);
        if (!AgentProcessStatus.valid(request.getStatus())) {
            throw new IllegalArgumentException("Unsupported agent process status");
        }
        if (request.getStatusReportTime() == null) {
            request.setStatusReportTime(new Date());
        }
        long reportedAt = request.getStatusReportTime().getTime();
        if (entity.getStatusReportTime() != null && reportedAt < entity.getStatusReportTime()) {
            request.setStatusReportTime(new Date());
        }

        entity.setPid(request.getPid());
        entity.setStatus(request.getStatus());
        entity.setStatusReportTime(request.getStatusReportTime().getTime());
        entity.setCpuUsage(request.getCpuUsage());
        entity.setMemoryUsage(request.getMemoryUsage());
        entity.setErrorReason(request.getErrorReason());
        repository.saveAgentProcess(entity);
    }

    /**
     * The Reader a status report belongs to, found by its agent id.
     *
     * <p>Authentication is the framework's job: the endpoint sits behind the same login
     * resolution as every other TM route, so a report that reaches this method has already
     * been authenticated. What remains here is the lookup the report needs.
     */
    private AgentProcessEntity resolveReportingEntity(AgentProcessStatusDto request) {
        return repository.findByAgentId(request.getAgentId()).orElseThrow(
                () -> new IllegalArgumentException("Agent process is not registered"));
    }

    @Override
    public String effectiveStatus(AgentProcessEntity entity) {
        if (entity == null) {
            return AgentProcessStatus.OFFLINE;
        }
        if (AgentProcessStatus.FAILED.equals(entity.getStatus())) {
            return AgentProcessStatus.FAILED;
        }
        if (entity.getStatusReportTime() == null
                || System.currentTimeMillis() - entity.getStatusReportTime() > OFFLINE_THRESHOLD_MILLIS) {
            return AgentProcessStatus.OFFLINE;
        }
        return StringUtils.defaultIfBlank(entity.getStatus(), AgentProcessStatus.OFFLINE);
    }

    @Override
    public AgentProcessDto toDto(AgentProcessEntity entity) {
        if (entity == null) {
            return null;
        }
        AgentProcessDto dto = entity instanceof OracleLogParserAgentProcessEntity
                ? new OracleLogParserAgentProcessDto()
                : new AgentProcessDto();
        dto.setProxyProcessId(entity.getProxyProcessId());
        dto.setAgentName(entity.getAgentName());
        dto.setAgentType(entity.getAgentType());
        dto.setAgentId(entity.getAgentId());
        if (dto instanceof OracleLogParserAgentProcessDto oracleDto
                && entity instanceof OracleLogParserAgentProcessEntity oracleEntity) {
            oracleDto.setSourceServiceId(oracleEntity.getSourceServiceId());
            oracleDto.setSourceState(oracleEntity.getSourceState());
        }
        dto.setAdvertiseIp(entity.getAdvertiseIp());
        dto.setAdvertisePort(entity.getAdvertisePort());
        dto.setStartTime(toDate(entity.getStartTime()));
        dto.setStatus(effectiveStatus(entity));
        dto.setStatusReportTime(toDate(entity.getStatusReportTime()));
        dto.setPid(entity.getPid());
        dto.setCpuUsage(entity.getCpuUsage());
        dto.setMemoryUsage(entity.getMemoryUsage());
        dto.setErrorReason(entity.getErrorReason());
        dto.setLastRegisterTime(toDate(entity.getLastRegisterTime()));
        /*
         * The whole credential travels with the listing, because that is how a
         * connection learns how to obtain an access token: the selection UI
         * copies this object into `proxyProcessInfo`, so anything omitted here
         * is simply unavailable when the connection is configured. The secret is
         * the Reader's own credential, returned to a caller that could already
         * manage that Reader.
         */
        dto.setClientId(entity.getClientId());
        dto.setClientSecret(revealSecretQuietly(entity));
        return dto;
    }

    /**
     * The reported secret, or {@code null} when this Reader has none or the stored
     * copy cannot be decrypted.
     *
     * <p>Never throws: one Reader with an undecryptable secret (a rotated key, a
     * row predating the feature) must not break the listing for everyone.
     */
    private String revealSecretQuietly(AgentProcessEntity entity) {
        if (agentCredentialService == null || StringUtils.isBlank(entity.getClientId())) {
            return null;
        }
        try {
            return agentCredentialService.revealSecret(entity);
        } catch (RuntimeException e) {
            log.warn("Unable to reveal the client secret for agent {}: {}",
                    entity.getAgentId(), e.getMessage());
            return null;
        }
    }

    @Override
    public Page<AgentProcessDto> list(AgentProcessQueryDto request) {
        AgentProcessQueryDto queryDto = request == null ? new AgentProcessQueryDto() : request;
        Query query = new Query();
        if (StringUtils.isNotBlank(queryDto.getProxyProcessId())) {
            if (!ObjectId.isValid(queryDto.getProxyProcessId())) {
                return new Page<>(0, List.of());
            }
            query.addCriteria(Criteria.where("_id").is(new ObjectId(queryDto.getProxyProcessId())));
        }
        if (StringUtils.isNotBlank(queryDto.getAgentType())) {
            query.addCriteria(Criteria.where("agentType").is(queryDto.getAgentType()));
        }
        List<AgentProcessDto> all = new ArrayList<>();
        List<AgentProcessEntity> entities = AgentProcessType.ORACLE_LOG_PARSER.getValue()
                .equals(queryDto.getAgentType())
                ? repository.findOracleLogParserProjections(query)
                : repository.findAll(query);
        entities.forEach(entity -> all.add(toDto(entity)));
        if (StringUtils.isNotBlank(queryDto.getProxyProcessId())) {
            all.removeIf(item -> !StringUtils.equals(queryDto.getProxyProcessId(), item.getProxyProcessId()));
        }
        if (StringUtils.isNotBlank(queryDto.getStatus())) {
            all.removeIf(item -> !queryDto.getStatus().equals(item.getStatus()));
        }

        String sortBy = StringUtils.defaultIfBlank(queryDto.getSortBy(), "lastRegisterTime");
        String sortOrder = StringUtils.defaultIfBlank(queryDto.getSortOrder(), "DESC");
        if (!"ASC".equalsIgnoreCase(sortOrder) && !"DESC".equalsIgnoreCase(sortOrder)) {
            throw new IllegalArgumentException("sortOrder must be ASC or DESC");
        }
        Comparator<AgentProcessDto> comparator = (left, right) ->
                compareValues(sortValue(left, sortBy), sortValue(right, sortBy));
        if ("DESC".equalsIgnoreCase(sortOrder)) {
            comparator = comparator.reversed();
        }
        all.sort(comparator);

        int page = queryDto.getPage() == null || queryDto.getPage() < 1 ? 1 : queryDto.getPage();
        int pageSize = queryDto.getPageSize() == null || queryDto.getPageSize() < 1 ? 20 : queryDto.getPageSize();
        int from = Math.min((page - 1) * pageSize, all.size());
        int to = Math.min(from + pageSize, all.size());
        return new Page<>(all.size(), all.subList(from, to));
    }

    @Override
    public List<String> listAgentTypes() {
        return repository.findAgentTypes();
    }

    @Override
    public void delete(String proxyProcessId) {
        if (StringUtils.isBlank(proxyProcessId) || !ObjectId.isValid(proxyProcessId)) {
            throw new IllegalArgumentException("Invalid proxyProcessId");
        }
        /*
         * Read the client id before deleting so the OAuth2 client goes with the registration;
         * otherwise a removed Reader could keep minting access tokens with a credential TM no
         * longer has a record of.
         */
        String clientId = repository.findById(new ObjectId(proxyProcessId))
                .map(AgentProcessEntity::getClientId).orElse(null);
        if (!repository.deleteById(new ObjectId(proxyProcessId))) {
            throw new IllegalArgumentException("Agent process not found");
        }
        if (StringUtils.isNotBlank(clientId) && agentCredentialService != null) {
            agentCredentialService.revokeAgent(clientId);
        }
    }

    @Override
    public String revealClientSecret(String proxyProcessId) {
        if (StringUtils.isBlank(proxyProcessId) || !ObjectId.isValid(proxyProcessId)) {
            throw new IllegalArgumentException("Invalid proxyProcessId");
        }
        AgentProcessEntity entity = repository.findById(new ObjectId(proxyProcessId))
                .orElseThrow(() -> new IllegalArgumentException("Agent process not found"));
        if (StringUtils.isBlank(entity.getClientId())) {
            return null;
        }
        if (agentCredentialService == null) {
            return null;
        }
        String secret = agentCredentialService.revealSecret(entity);
        if (secret == null) {
            /*
             * Either the key rotated or the row predates this feature. The Reader still holds the
             * secret in its manager.env, so the operator is told to re-read it there rather than
             * being handed a wrong value.
             */
            throw new IllegalStateException(
                    "Stored client secret cannot be decrypted; read it from the Reader manager.env");
        }
        return secret;
    }

    private Object sortValue(AgentProcessDto dto, String sortBy) {
        return switch (sortBy) {
            case "agentName" -> dto.getAgentName();
            case "agentType" -> dto.getAgentType();
            case "agentId" -> dto.getAgentId();
            case "status" -> dto.getStatus();
            case "statusReportTime" -> dto.getStatusReportTime();
            case "pid" -> dto.getPid();
            case "cpuUsage" -> dto.getCpuUsage();
            case "memoryUsage" -> dto.getMemoryUsage();
            case "startTime" -> dto.getStartTime();
            case "lastRegisterTime" -> dto.getLastRegisterTime();
            default -> throw new IllegalArgumentException("Unsupported sortBy: " + sortBy);
        };
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private int compareValues(Object left, Object right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        return ((Comparable) left).compareTo(right);
    }

    private void validateRegisterRequest(AgentProcessRegisterDto request) {
        if (request == null) {
            throw new IllegalArgumentException("Agent process registration parameters are incomplete");
        }
        if (StringUtils.isBlank(request.getAgentId())) {
            throw new IllegalArgumentException("Agent process registration parameters are incomplete: agentId");
        }
        if (StringUtils.isBlank(request.getAgentType())) {
            throw new IllegalArgumentException("Agent process registration parameters are incomplete: agentType");
        }
        if (StringUtils.isBlank(request.getAdvertiseIp())) {
            throw new IllegalArgumentException("Agent process registration parameters are incomplete: advertiseIp");
        }
        if (request.getAdvertisePort() == null || request.getAdvertisePort() <= 0 || request.getAdvertisePort() > 65535) {
            throw new IllegalArgumentException("Agent process registration parameters are incomplete: advertisePort");
        }
        if (request.getStartTime() == null) {
            request.setStartTime(new Date());
        }
    }

    private void validateStatusRequest(AgentProcessStatusDto request) {
        if (request == null || StringUtils.isBlank(request.getAgentId())
                || request.getStartTime() == null || request.getPid() == null
                || StringUtils.isBlank(request.getStatus())) {
            throw new IllegalArgumentException("Agent process status parameters are incomplete");
        }
    }

    private boolean sameStartTime(Long left, Date right) {
        return left != null && right != null && left.longValue() == right.getTime();
    }

    private Date toDate(Long timestamp) {
        return timestamp == null ? null : new Date(timestamp);
    }
}
