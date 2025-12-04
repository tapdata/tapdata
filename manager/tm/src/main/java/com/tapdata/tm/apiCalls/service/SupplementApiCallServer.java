package com.tapdata.tm.apiCalls.service;

import com.tapdata.tm.apiCalls.dto.ApiCallDto;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.vo.WorkerCallsInfo;
import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.apiServer.utils.PercentileCalculator;
import com.tapdata.tm.apicallminutestats.dto.ApiCallMinuteStatsDto;
import com.tapdata.tm.apicallminutestats.entity.ApiCallMinuteStatsEntity;
import com.tapdata.tm.apicallminutestats.service.ApiCallMinuteStatsService;
import com.tapdata.tm.apicallstats.dto.ApiCallStatsDto;
import com.tapdata.tm.apicallstats.entity.ApiCallStatsEntity;
import com.tapdata.tm.apicallstats.service.ApiCallStatsService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/26 16:20 Create
 * @description
 */
@Service
@Slf4j
public class SupplementApiCallServer {
    public static final String API_CALL = MongoUtils.getCollectionName(ApiCallEntity.class);
    public static final String SUPPLEMENT = "supplement";
    public static final String MODULE_ID = "moduleId";

    @Value("${api-server.supplement-hour:16}")
    Integer supplementHour;

    @Autowired
    MongoTemplate mongoOperations;
    @Autowired
    WorkerCallServiceImpl workerCallService;
    @Autowired
    ApiCallMinuteStatsService apiCallMinuteStatsService;
    @Autowired
    ApiCallStatsService apiCallStatsService;

    public void supplementOnce() {
        LocalDateTime filterTime = LocalDateTime.now().minusHours(supplementHour);
        Criteria criteria = Criteria.where(SUPPLEMENT).is(true)
                .and("createTime").gte(filterTime)
                .and("allPathId").ne(null)
                .and("reqTime").ne(null);
        Query query = Query.query(criteria);
        long count = mongoOperations.count(query, ApiCallDto.class, API_CALL);
        if (count <= 0L) {
            return;
        }
        int from = 0;
        int size = 1000;
        query.with(Sort.by(Sort.Order.asc("_id")));
        ObjectId lastId = null;
        try (WorkerCallsInfoGenerator generator = new WorkerCallsInfoGenerator(this::callUpdate)) {
            List<ApiCallDto> apiCalls = null;
            do {
                if (lastId != null) {
                    query = Query.query(
                            Criteria.where(SUPPLEMENT).is(true)
                                    .and("createTime").gte(filterTime)
                                    .and("allPathId").ne(null)
                                    .and("reqTime").ne(null)
                                    .and("_id").gt(lastId)
                    ).with(Sort.by(Sort.Order.asc("_id"))).limit(size);
                }
                apiCalls = mongoOperations.find(query, ApiCallDto.class, API_CALL);
                from = from + size;
                if (CollectionUtils.isEmpty(apiCalls)) {
                    break;
                }
                lastId = apiCalls.get(apiCalls.size() - 1).getId();
                List<ObjectId> supplementIds = new ArrayList<>(16);
                apiCalls.forEach(apiCall -> supplementIds.add(apiCall.getId()));
                acceptBatch(apiCalls, generator);
                foreach(apiCalls);
                Query updateFilter = Query.query(Criteria.where("_id").in(supplementIds));
                mongoOperations.updateMulti(updateFilter, Update.update(SUPPLEMENT, false), API_CALL);
            } while (CollectionUtils.isEmpty(apiCalls) || from > count - 1);
        }
    }

    protected void acceptBatch(List<ApiCallDto> saveApiCallParamList, WorkerCallsInfoGenerator generator) {
        Map<String, Map<String, List<WorkerCallsInfo>>> collect = saveApiCallParamList.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getWorkOid()) && StringUtils.isNotBlank(e.getAllPathId()))
                .collect(Collectors.groupingBy(
                        ApiCallDto::getWorkOid,
                        Collectors.groupingBy(
                                ApiCallDto::getAllPathId,
                                Collectors.mapping(
                                        this::convertToWorkerCallsInfo,
                                        Collectors.collectingAndThen(
                                                Collectors.toList(),
                                                this::sortAndProcessList
                                        )
                                )
                        )
                ));
        collect.forEach((workerId, apiIdMap) ->
                apiIdMap.forEach((apiId, infos) ->
                        generator.append(infos)
                )
        );
    }

    protected void foreach(List<ApiCallDto> apiCalls) {
        if (CollectionUtils.isEmpty(apiCalls)) {
            return;
        }
        Map<String, ApiCallStatsDto> callMap = new HashMap<>(16);
        Map<String, ApiCallMinuteStatsDto> callMinuteMap = new HashMap<>(16);
        List<String> apiIds = apiCalls.stream()
                .filter(Objects::nonNull)
                .map(ApiCallDto::getAllPathId)
                .filter(StringUtils::isNotBlank)
                .distinct()
                .toList();
        List<ApiCallStatsDto> callStatus = mongoOperations.find(Query.query(Criteria.where(MODULE_ID).in(apiIds)), ApiCallStatsDto.class, MongoUtils.getCollectionName(ApiCallStatsEntity.class));
        callStatus.forEach(e -> callMap.put(e.getModuleId(), e));
        Map<String, Criteria> collect = apiCalls.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getAllPathId()))
                .filter(e -> Objects.nonNull(e.getReqTime()))
                .collect(Collectors.groupingBy(
                        ApiCallDto::getAllPathId,
                        Collectors.collectingAndThen(Collectors.toList(), list -> {
                            ApiCallDto apiCallDto = list.get(0);
                            List<Date> times = list.stream()
                                    .map(ApiCallDto::getReqTime)
                                    .map(time -> time / 60000L * 60000L)
                                    .distinct()
                                    .map(Date::new)
                                    .toList();
                            return Criteria.where(MODULE_ID).is(apiCallDto.getAllPathId()).and("apiCallTime").in(times);
                        })
                ));
        if (CollectionUtils.isEmpty(collect)) {
            return;
        }
        List<ApiCallMinuteStatsDto> minuteStats = mongoOperations.find(Query.query(new Criteria().orOperator(collect.values())), ApiCallMinuteStatsDto.class, MongoUtils.getCollectionName(ApiCallMinuteStatsEntity.class));
        minuteStats.forEach(e -> callMinuteMap.put(e.getModuleId() + "_" + e.getApiCallTime().getTime(), e));
        apiCalls.forEach(apiCall -> {
            acceptApiCallStats(apiCall, callMap);
            acceptApiCallMinuteStats(apiCall, callMinuteMap);
        });
        if (!callMap.isEmpty()) {
            apiCallStatsService.bulkWrite(new ArrayList<>(callMap.values()), ApiCallStatsEntity.class, entity -> {
                Criteria criteria = Criteria.where(MODULE_ID).is(entity.getModuleId());
                return Query.query(criteria);
            });
        }
        if (!callMinuteMap.isEmpty()) {
            apiCallMinuteStatsService.bulkWrite(new ArrayList<>(callMinuteMap.values()), ApiCallMinuteStatsEntity.class, entity -> {
                Criteria criteria = Criteria.where(MODULE_ID).is(entity.getModuleId())
                        .and("apiCallTime").is(entity.getApiCallTime());
                return Query.query(criteria);
            });
        }
    }

    protected void acceptApiCallStats(ApiCallDto apiCall, Map<String, ApiCallStatsDto> callMap) {
        String apiId = apiCall.getAllPathId();
        ApiCallStatsDto callInfo = callMap.computeIfAbsent(apiId, k -> {
            ApiCallStatsDto callItem = new ApiCallStatsDto();
            callItem.setModuleId(apiId);
            return callItem;
        });
        long totalCount = Optional.ofNullable(callInfo.getCallTotalCount()).orElse(0L) + 1L;
        callInfo.setCallTotalCount(totalCount);
        long bytes = Optional.ofNullable(callInfo.getTransferDataTotalBytes()).orElse(0L) + Optional.ofNullable(apiCall.getReqBytes()).orElse(0L);
        callInfo.setTransferDataTotalBytes(bytes);
        if (!"OK".equals(apiCall.getCodeMsg())) {
            callInfo.setCallAlarmTotalCount(Optional.ofNullable(callInfo.getCallAlarmTotalCount()).orElse(0L) + 1L);
        }
        long rows = Optional.ofNullable(callInfo.getResponseDataRowTotalCount()).orElse(0L) + Optional.ofNullable(apiCall.getResRows()).orElse(0L);
        callInfo.setResponseDataRowTotalCount(rows);
        Long latency = Optional.ofNullable(apiCall.getLatency()).orElse(0L);
        long time = Optional.ofNullable(callInfo.getTotalResponseTime()).orElse(0L) + latency;
        callInfo.setTotalResponseTime(time);
        callInfo.setAccessFailureRate(1.0D * Optional.ofNullable(callInfo.getCallAlarmTotalCount()).orElse(0L) / totalCount);
        callInfo.setMaxResponseTime(Math.max(Optional.ofNullable(callInfo.getMaxResponseTime()).orElse(0L), latency));
        callInfo.setAlarmApiTotalCount(callInfo.getCallAlarmTotalCount());
        Optional.ofNullable(apiCall.getUserInfo())
                .map(e -> e.get("clientId"))
                .map(String::valueOf)
                .ifPresent(clientId -> {
                    Set<String> clientIdSet = Optional.ofNullable(callInfo.getClientIds()).orElse(new HashSet<>());
                    clientIdSet.add(clientId);
                    callInfo.setClientIds(clientIdSet);
                });
        if (null == callInfo.getLastApiCallId() || apiCall.getId().compareTo(MongoUtils.toObjectId(callInfo.getLastApiCallId())) > 0) {
            callInfo.setLastApiCallId(apiCall.getId().toHexString());
        }
    }

    protected void acceptApiCallMinuteStats(ApiCallDto apiCall, Map<String, ApiCallMinuteStatsDto> callMinuteMap) {
        String apiId = apiCall.getAllPathId();
        if (null == apiCall.getReqTime()) {
            return;
        }
        long minute = apiCall.getReqTime() / 60000L * 60000L;
        ApiCallMinuteStatsDto callInfo = callMinuteMap.computeIfAbsent(String.format("%s_%d", apiId, minute), k -> {
            ApiCallMinuteStatsDto callItem = new ApiCallMinuteStatsDto();
            callItem.setModuleId(apiId);
            callItem.setApiCallTime(new Date(minute));
            return callItem;
        });
        long countBefore = Optional.ofNullable(callInfo.getResponseDataRowTotalCount()).orElse(0L);
        long rowCount = countBefore + Optional.ofNullable(apiCall.getResRows()).orElse(0L);
        callInfo.setResponseDataRowTotalCount(rowCount);
        long latency = Optional.ofNullable(apiCall.getLatency()).orElse(0L);
        long time = Optional.ofNullable(callInfo.getTotalResponseTime()).orElse(0L) + latency;
        callInfo.setTotalResponseTime(time);
        long bytesBefore = Optional.ofNullable(callInfo.getTransferDataTotalBytes()).orElse(0L);
        long bytes = bytesBefore + Optional.ofNullable(apiCall.getReqBytes()).orElse(0L);
        callInfo.setTransferDataTotalBytes(bytes);
        callInfo.setResponseTimePerRow(callInfo.getResponseDataRowTotalCount() == 0L ? 0D : (1.0D * callInfo.getTotalResponseTime() / callInfo.getResponseDataRowTotalCount()));
        double transferBytePerSecond = callInfo.getTotalResponseTime() == 0L ? 0D : (1.0D * callInfo.getTransferDataTotalBytes() / callInfo.getTotalResponseTime() * 1000D);
        callInfo.setTransferBytePerSecond(transferBytePerSecond);
        if (null == callInfo.getLastApiCallId() || apiCall.getId().compareTo(MongoUtils.toObjectId(callInfo.getLastApiCallId())) > 0) {
            callInfo.setLastApiCallId(apiCall.getId().toHexString());
        }
    }

    private WorkerCallsInfo convertToWorkerCallsInfo(ApiCallDto dto) {
        WorkerCallsInfo callsInfo = new WorkerCallsInfo();
        callsInfo.setWorkOid(dto.getWorkOid());
        callsInfo.setApiGatewayUuid(dto.getApi_gateway_uuid());
        callsInfo.setApiId(dto.getAllPathId());
        callsInfo.setLatency(dto.getLatency());
        callsInfo.setCode(dto.getCode());
        callsInfo.setReqTime(dto.getReqTime());
        callsInfo.setResTime(dto.getResTime());
        return callsInfo;
    }

    private List<WorkerCallsInfo> sortAndProcessList(List<WorkerCallsInfo> list) {
        return list.stream()
                .sorted(Comparator.comparing(WorkerCallsInfo::getReqTime))
                .toList();
    }

    protected void callUpdate(List<WorkerCallEntity> infos) {
        if (CollectionUtils.isEmpty(infos)) {
            return;
        }
        List<Criteria> or = new ArrayList<>();
        for (WorkerCallEntity entity : infos) {
            Criteria criteria = Criteria.where(WorkerCallServiceImpl.Tag.TIME_START).is(entity.getTimeStart())
                    .and(WorkerCallServiceImpl.Tag.DELETE).is(entity.getDelete())
                    .and(WorkerCallServiceImpl.Tag.TIME_GRANULARITY).is(entity.getTimeGranularity())
                    .and(WorkerCallServiceImpl.Tag.PROCESS_ID).is(entity.getProcessId())
                    .and(WorkerCallServiceImpl.Tag.API_ID).is(entity.getApiId())
                    .and(WorkerCallServiceImpl.Tag.WORK_OID).is(entity.getWorkOid());
            or.add(criteria);
        }
        Query query = Query.query(new Criteria().orOperator(or));
        List<WorkerCallEntity> entities = mongoOperations.find(query, WorkerCallEntity.class);
        Map<String, Map<String, List<WorkerCallEntity>>> collect = entities.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(
                        WorkerCallEntity::getWorkOid,
                        Collectors.groupingBy(
                                WorkerCallEntity::getApiId
                        )
                ));
        infos.forEach(entity -> {
                    Optional.ofNullable(collect.get(entity.getWorkOid()))
                            .map(e -> e.get(entity.getApiId()))
                            .ifPresent(vos -> vos.forEach(item -> {
                                entity.setReqCount(Optional.ofNullable(entity.getErrorCount()).orElse(0L) + Optional.ofNullable(item.getErrorCount()).orElse(0L));
                                entity.setReqCount(Optional.ofNullable(entity.getReqCount()).orElse(0L) + Optional.ofNullable(item.getReqCount()).orElse(0L));
                                Optional.ofNullable(item.getDelays()).ifPresent(ds -> {
                                    List<Long> delays = Optional.ofNullable(entity.getDelays()).orElse(new ArrayList<>());
                                    delays.addAll(ds);
                                    entity.setDelays(delays);
                                });
                            }));
                    Long p50 = PercentileCalculator.calculatePercentile(entity.getDelays(), 0.5d);
                    Long p95 = PercentileCalculator.calculatePercentile(entity.getDelays(), 0.95d);
                    Long p99 = PercentileCalculator.calculatePercentile(entity.getDelays(), 0.99d);
                    long errorCount = Optional.ofNullable(entity.getErrorCount()).orElse(0L);
                    long reqCount = Optional.ofNullable(entity.getReqCount()).orElse(0L);
                    Double errorRate = reqCount <= 0 ? 0 : (0.1d * errorCount / reqCount);
                    entity.setReqCount(reqCount);
                    Double rps = reqCount > 0L ? reqCount * 1.0D / 60D : 0D;
                    entity.setP50(p50);
                    entity.setP95(p95);
                    entity.setP99(p99);
                    entity.setErrorRate(errorRate);
                    entity.setRps(rps);
                }
        );
        workerCallService.bulkUpsert(infos);
    }
}
