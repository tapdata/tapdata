package com.tapdata.tm.proxyprocess.service.impl;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.proxyprocess.constant.AgentProcessStatus;
import com.tapdata.tm.proxyprocess.dto.AgentProcessDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessQueryDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterResponseDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessStatusDto;
import com.tapdata.tm.proxyprocess.entity.AgentProcessEntity;
import com.tapdata.tm.proxyprocess.repository.AgentProcessRepository;
import com.tapdata.tm.proxyprocess.service.AgentProcessService;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class AgentProcessServiceImpl implements AgentProcessService {
    static final long OFFLINE_THRESHOLD_MILLIS = 5 * 60 * 1000L;

    private final AgentProcessRepository repository;
    @Autowired
    public AgentProcessServiceImpl(AgentProcessRepository repository) {
        this.repository = repository;
    }

    @Override
    public AgentProcessRegisterResponseDto register(AgentProcessRegisterDto request) {
        validateRegisterRequest(request);
        Optional<AgentProcessEntity> existingOptional = repository.findByAgentId(request.getAgentId());
        AgentProcessEntity entity = existingOptional.orElseGet(AgentProcessEntity::new);
        boolean sameProcess = sameStartTime(entity.getStartTime(), request.getStartTime());

        entity.setAgentName(request.getAgentName());
        entity.setAgentType(request.getAgentType());
        entity.setAgentId(request.getAgentId());
        entity.setAdvertiseIp(request.getAdvertiseIp());
        entity.setAdvertisePort(request.getAdvertisePort());
        entity.setStartTime(request.getStartTime().getTime());
        entity.setLastRegisterTime(System.currentTimeMillis());
        if (!sameProcess || StringUtils.isBlank(entity.getToken())) {
            entity.setToken(UUID.randomUUID().toString().replace("-", ""));
            entity.setStatus(null);
            entity.setStatusReportTime(null);
            entity.setPid(null);
            entity.setCpuUsage(null);
            entity.setMemoryUsage(null);
            entity.setErrorReason(null);
        }

        repository.saveAgentProcess(entity);
        AgentProcessRegisterResponseDto response = new AgentProcessRegisterResponseDto();
        response.setProxyProcessId(entity.getProxyProcessId());
        response.setAgentId(entity.getAgentId());
        response.setToken(entity.getToken());
        return response;
    }

    @Override
    public void reportStatus(AgentProcessStatusDto request, String authorization) {
        validateStatusRequest(request);
        String token = extractToken(authorization);
        AgentProcessEntity entity = repository.findByToken(token)
                .orElseThrow(() -> new IllegalArgumentException("Invalid agent process token"));
        if (!StringUtils.equals(entity.getAgentId(), request.getAgentId())
                || !sameStartTime(entity.getStartTime(), request.getStartTime())) {
            throw new IllegalArgumentException("Agent process identity does not match token");
        }
        if (!AgentProcessStatus.RUNNING.equals(request.getStatus())
                && !AgentProcessStatus.FAILED.equals(request.getStatus())) {
            throw new IllegalArgumentException("Unsupported agent process status");
        }
        if (request.getStatusReportTime() == null) {
            throw new IllegalArgumentException("statusReportTime is required");
        }

        entity.setPid(request.getPid());
        entity.setStatus(request.getStatus());
        entity.setStatusReportTime(System.currentTimeMillis());
        entity.setCpuUsage(request.getCpuUsage());
        entity.setMemoryUsage(request.getMemoryUsage());
        entity.setErrorReason(request.getErrorReason());
        repository.saveAgentProcess(entity);
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
        AgentProcessDto dto = new AgentProcessDto();
        dto.setProxyProcessId(entity.getProxyProcessId());
        dto.setAgentName(entity.getAgentName());
        dto.setAgentType(entity.getAgentType());
        dto.setAgentId(entity.getAgentId());
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
        return dto;
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
        repository.findAll(query).forEach(entity -> all.add(toDto(entity)));
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
    public void delete(String proxyProcessId) {
        if (StringUtils.isBlank(proxyProcessId) || !ObjectId.isValid(proxyProcessId)) {
            throw new IllegalArgumentException("Invalid proxyProcessId");
        }
        if (!repository.deleteById(new ObjectId(proxyProcessId))) {
            throw new IllegalArgumentException("Agent process not found");
        }
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
        if (request == null || StringUtils.isBlank(request.getAgentId())
                || StringUtils.isBlank(request.getAgentType())
                || StringUtils.isBlank(request.getAdvertiseIp())
                || request.getAdvertisePort() == null || request.getAdvertisePort() <= 0
                || request.getAdvertisePort() > 65535
                || request.getStartTime() == null) {
            throw new IllegalArgumentException("Agent process registration parameters are incomplete");
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

    private String extractToken(String authorization) {
        if (StringUtils.isBlank(authorization)) {
            throw new IllegalArgumentException("Agent process token is required");
        }
        String value = authorization.trim();
        if (value.regionMatches(true, 0, "Bearer ", 0, 7)) {
            value = value.substring(7).trim();
        }
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException("Agent process token is required");
        }
        return value;
    }
}
