package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.service.WorkerCallServiceImpl;
import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.v2.api.common.main.dto.TimeRange;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import com.tapdata.tm.v2.api.monitor.main.param.TopWorkerInServerParam;
import com.tapdata.tm.v2.api.monitor.repository.ApiMetricsRepository;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsDelayInfoUtil;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 10:11 Create
 * @description
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class ApiMetricsRawService {
    private ApiMetricsRepository repository;
    private MongoTemplate mongoTemplate;

    public long count(Query query) {
        return repository.count(query);
    }


    public List<ApiMetricsRaw> find(Query query) {
        return repository.findAll(query);
    }

    public List<WorkerCallEntity> supplementWorkerCall(TopWorkerInServerParam param) {
        String serverId = param.getServerId();
        long endAt = param.getEndAt() * 1000L;
        if (endAt % 60000L != 0L) {
            endAt = endAt / 60000L * 60000L;
        }
        long startAt = param.getStartAt() * 1000L;
        if (startAt % 60000L != 0L) {
            startAt = (startAt + 59999L) / 60000L * 60000L;
        }
        List<WorkerCallEntity> callOfWorker = new ArrayList<>();
        if (startAt < endAt) {
            Criteria criteriaOfWorker = Criteria.where(WorkerCallServiceImpl.Tag.PROCESS_ID).is(serverId)
                    .and(WorkerCallServiceImpl.Tag.TIME_START).gte(startAt).lt(endAt)
                    .and(WorkerCallServiceImpl.Tag.TIME_GRANULARITY).is(1)
                    .and(WorkerCallServiceImpl.Tag.DELETE).ne(true);
            List<WorkerCallEntity> callOfMinute = mongoTemplate.find(Query.query(criteriaOfWorker), WorkerCallEntity.class, "ApiCallInWorker");
            if (!callOfMinute.isEmpty()) {
                callOfWorker.addAll(callOfMinute);
            }
        }
        List<TimeRange> secondPoint = new ArrayList<>();
        long start = param.getStartAt() * 1000L;
        long end = param.getEndAt() * 1000L;
        if (startAt > start) {
            secondPoint.add(new TimeRange(start, startAt, TimeGranularity.SECOND));
        }
        if (end > endAt) {
            secondPoint.add(new TimeRange(endAt, end, TimeGranularity.SECOND));
        }
        if (!secondPoint.isEmpty()) {
            Criteria criteriaOfSec = Criteria.where("api_gateway_uuid").is(serverId)
                    .and("delete").ne(true);
            List<Criteria> or = new ArrayList<>();
            for (TimeRange point : secondPoint) {
                or.add(Criteria.where("reqTime").gte(point.getStart()).lt(point.getEnd()));
            }
            criteriaOfSec.orOperator(or);
            Query query = Query.query(criteriaOfSec);
            String callName = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
            if (StringUtils.isNotBlank(callName)) {
                List<ApiCallEntity> calls = mongoTemplate.find(query, ApiCallEntity.class, callName);
                Map<String, List<ApiCallEntity>> collect = calls.stream()
                        .filter(Objects::nonNull)
                        .filter(callItem -> StringUtils.isNotBlank(callItem.getWorkOid()))
                        .filter(e -> StringUtils.isNotBlank(e.getReq_path()))
                        .filter(e -> !MetricInstanceFactory.IGNORE_PATH.contains(e.getReq_path()))
                        .collect(Collectors.groupingBy(ApiCallEntity::getWorkOid));
                long finalEndAt = endAt;
                long finalStartAt = startAt;
                collect.forEach((oId, callItems) -> {
                    WorkerCallEntity left = one(serverId, oId, start);
                    WorkerCallEntity right = one(serverId, oId, finalEndAt);
                    for (ApiCallEntity call : callItems) {
                        Long reqTime = call.getReqTime();
                        if (null == reqTime) {
                            continue;
                        }
                        if (reqTime >= start && reqTime < finalStartAt) {
                            merge(left, call);
                        } else if (reqTime >= finalEndAt && reqTime < end) {
                            merge(right, call);
                        }
                    }
                    end(left);
                    end(right);
                    callOfWorker.add(0, left);
                    callOfWorker.add(right);
                });
            }
        }
        return callOfWorker;
    }

    void end(WorkerCallEntity one) {
        List<Map<String, Number>> merged = one.getDelays();
        long reqCount = ApiMetricsDelayUtil.sum(merged).getCount();
        Long p50L = ApiMetricsDelayUtil.p50(merged, reqCount);
        Long p95L = ApiMetricsDelayUtil.p95(merged, reqCount);
        Long p99L = ApiMetricsDelayUtil.p99(merged, reqCount);
        one.setP50(p50L);
        one.setP95(p95L);
        one.setP99(p99L);
    }

    void merge(WorkerCallEntity one, ApiCallEntity entity) {
        one.setReqCount(one.getReqCount() + 1L);
        one.setRps(one.getReqCount() / 60D);
        List<Map<String, Number>> merged = one.getDelays();
        merged = ApiMetricsDelayUtil.addDelay(merged, entity.getLatency());
        one.setDelays(merged);
        boolean isOk = ApiMetricsDelayInfoUtil.checkByCode(entity.getCode(), entity.getHttpStatus());
        one.setErrorCount(one.getErrorCount() + (isOk ? 0L : 1L));
        one.setErrorRate(1D * one.getErrorCount() / one.getReqCount());
    }

    WorkerCallEntity one(String serverId, String oId, Long time) {
        WorkerCallEntity one = new WorkerCallEntity();
        one.setProcessId(serverId);
        one.setWorkOid(oId);
        one.setReqCount(0L);
        one.setRps(0D);
        one.setDelays(new ArrayList<>());
        one.setErrorCount(0L);
        one.setErrorRate(0D);
        one.setTimeStart(time / 60L * 60L);
        one.setTimeGranularity(1);
        one.setId(new ObjectId());
        return one;
    }

    protected void supplementSeconds(Criteria apiCallCriteria, List<ApiMetricsRaw> supplement, List<TimeRange> ranges) {
        List<Criteria> andCriteria = new ArrayList<>();
        andCriteria.add(Criteria.where("delete").is(false));
        Optional.ofNullable(apiCallCriteria).ifPresent(andCriteria::add);
        if (!ranges.isEmpty()) {
            if (ranges.size() == 1) {
                TimeRange point = ranges.get(0);
                andCriteria.add(Criteria.where("reqTime").gte(point.getStart() * 1000L).lt(point.getEnd() * 1000L));
            } else {
                List<Criteria> orSec = ranges.stream()
                        .map(point -> Criteria.where("reqTime").gte(point.getStart() * 1000L).lt(point.getEnd() * 1000L))
                        .toList();
                andCriteria.add(new Criteria().orOperator(orSec));
            }
        }
        Query query = Query.query(new Criteria().andOperator(andCriteria));
        query.fields().include("api_gateway_uuid", "allPathId", "req_path", "reqTime", "code", "httpStatus", "req_bytes", "latency", "_id");
        String callName = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
        if (StringUtils.isNotBlank(callName)) {
            List<ApiCallEntity> calls = mongoTemplate.find(query, ApiCallEntity.class, callName);
            ParticleSizeAnalyzer.parseToMetric(calls, supplement::add);
        }
    }

    protected void supplementFiveSecond(Consumer<Criteria> criteriaConsumer, List<ApiMetricsRaw> supplement, List<TimeRange> ranges) {
        Criteria criteriaOfSec5 = Criteria.where("timeGranularity").is(1);
        Optional.ofNullable(criteriaConsumer).ifPresent(c -> c.accept(criteriaOfSec5));
        List<Criteria> or = new ArrayList<>();
        for (TimeRange point : ranges) {
            Criteria andCriteria = new Criteria();
            List<Criteria> and = new ArrayList<>();
            and.add(Criteria.where("timeStart").gte(point.getStart()));
            and.add(Criteria.where("timeStart").lt(point.getEnd()));
            andCriteria.andOperator(and);
            or.add(andCriteria);
        }
        criteriaOfSec5.orOperator(or);
        Query query = Query.query(criteriaOfSec5);
        query.fields().include("apiId", "processId", "timeGranularity", "timeStart", "subMetrics");
        List<ApiMetricsRaw> metricsRawList = find(query);
        for (TimeRange point : ranges) {
            List<ApiMetricsRaw> raws = ParticleSizeAnalyzer.secondFiveMetricsRaws(
                    metricsRawList,
                    e -> e.getTimeStart() >= point.getStart() && e.getTimeStart() < point.getEnd());
            supplement.addAll(raws);
        }
    }

    protected void supplementMinute(Consumer<Criteria> criteriaConsumer, List<ApiMetricsRaw> supplement, List<TimeRange> ranges, String[] filterFields) {
        Criteria criteriaOfMin = Criteria.where("timeGranularity").is(2);
        Optional.ofNullable(criteriaConsumer).ifPresent(c -> c.accept(criteriaOfMin));
        List<Criteria> or = new ArrayList<>();
        for (TimeRange point : ranges) {
            Criteria andCriteria = new Criteria();
            List<Criteria> and = new ArrayList<>();
            and.add(Criteria.where("timeStart").gte((point.getStart() / 60L * 60L)));
            and.add(Criteria.where("timeStart").lt(point.getEnd() / 60L * 60L));
            andCriteria.andOperator(and);
            or.add(andCriteria);
        }
        criteriaOfMin.orOperator(or);
        Query query = Query.query(criteriaOfMin);
        query.fields().include(filterFields);
        List<ApiMetricsRaw> raws = find(query);
        if (!raws.isEmpty()) {
            supplement.addAll(raws);
        }
    }

    public List<ApiMetricsRaw> supplementMetricsRaw(QueryBase param, boolean filterByTime, Consumer<Criteria> criteriaConsumer, Criteria apiCallCriteria, String[] filterFields) {
        TimeGranularity granularity = param.getGranularity();
        Map<TimeGranularity, List<TimeRange>> queryRange = param.getQueryRange();
        List<TimeRange> currentRanges = queryRange.get(granularity);
        List<ApiMetricsRaw> result = new ArrayList<>();
        if (!CollectionUtils.isEmpty(currentRanges)) {
            TimeGranularity queryTimeGranularity = granularity == TimeGranularity.SECOND_FIVE ? TimeGranularity.MINUTE : granularity;
            Criteria criteria = Criteria.where("timeGranularity").is(queryTimeGranularity.getType());
            criteriaConsumer.accept(criteria);
            List<Criteria> orTimeRange = new ArrayList<>();
            for (TimeRange range : currentRanges) {
                Criteria subRange;
                if (granularity == TimeGranularity.SECOND_FIVE) {
                    subRange = Criteria.where("timeStart")
                            .gte(TimeGranularity.MINUTE.fixTime(range.getStart()))
                            .lt(TimeGranularity.MINUTE.fixTime(range.getEnd() + TimeGranularity.MINUTE.getSeconds() - 1L));
                } else {
                    subRange = Criteria.where("timeStart").gte(range.getStart()).lt(range.getEnd());
                }
                orTimeRange.add(subRange);
            }
            criteria.andOperator(orTimeRange);
            Query query = new Query(criteria);
            if (granularity != TimeGranularity.SECOND_FIVE) {
                query.fields().include(filterFields);
            } else {
                query.fields().include("apiId", "processId", "timeGranularity", "timeStart", "subMetrics");
            }
            List<ApiMetricsRaw> apiMetricsRaws = find(query);
            if (granularity == TimeGranularity.SECOND_FIVE) {
                for (TimeRange range : currentRanges) {
                    List<ApiMetricsRaw> raws = ParticleSizeAnalyzer.secondFiveMetricsRaws(
                            apiMetricsRaws,
                            e -> e.getTimeStart() >= range.getStart() && e.getTimeStart() < range.getEnd()
                    );
                    if (!CollectionUtils.isEmpty(raws)) {
                        result.addAll(raws);
                    }
                }
            } else if (!apiMetricsRaws.isEmpty()) {
                result.addAll(apiMetricsRaws);
            }
        }

        List<ApiMetricsRaw> supplement = new ArrayList<>();
        /**
         * 小时级，补充边界上不足1秒，不足一分钟，不足一小时的数据
         * 分钟级，补充边界上不足1秒，不足一分钟的数据
         * 5秒级，补充边界上不足1秒的数据
         * */
        param.getQueryRange()
                .forEach((type, ranges) -> {
                    if (CollectionUtils.isEmpty(ranges)) {
                        return;
                    }
                    if (type == param.getGranularity()) {
                        return;
                    }
                    switch (granularity) {
                        case MINUTE:
                            supplementMinute(criteriaConsumer, supplement, ranges, filterFields);
                            break;
                        case SECOND_FIVE:
                            supplementFiveSecond(criteriaConsumer, supplement, ranges);
                            break;
                        case SECOND:
                            supplementSeconds(apiCallCriteria, supplement, ranges);
                            break;
                        default:
                            //do nothing
                    }
                });
        Map<String, ApiMetricsRaw> left = new HashMap<>();
        Map<String, ApiMetricsRaw> right = new HashMap<>();
        for (ApiMetricsRaw apiMetricsRaw : supplement) {
            String serverId = apiMetricsRaw.getProcessId();
            String apiId = apiMetricsRaw.getApiId();
            String key = String.format("%s_%s", serverId, apiId);
            Long timeStart = apiMetricsRaw.getTimeStart();
            if (null == timeStart) {
                continue;
            }
            if (timeStart >= param.getStartAt() && timeStart < param.getFixStart()) {
                merge(apiMetricsRaw, left, key, apiId, serverId, param);
            }
            if (timeStart >= param.getFixEnd() && timeStart < param.getEndAt()) {
                merge(apiMetricsRaw, right, key, apiId, serverId, param);
            }
        }
        long step = ApiMetricsDelayInfoUtil.stepByGranularity(param.getGranularity());
        if (!left.isEmpty()) {
            left.forEach((key, item) -> Optional.ofNullable(item)
                    .map(i -> {
                        i.setTimeStart(i.getTimeStart() / step * step);
                        return i;
                    })
                    .map(MetricInstanceAcceptor::calcPValue)
                    .ifPresent(i -> result.add(0, i)));
        }
        if (!CollectionUtils.isEmpty(right)) {
            right.forEach((key, item) -> Optional.ofNullable(item)
                    .map(i -> {
                        i.setTimeStart(i.getTimeStart() / step * step);
                        return i;
                    })
                    .map(MetricInstanceAcceptor::calcPValue)
                    .ifPresent(i -> result.add(0, i)));
        }
        return result.stream()
                .filter(Objects::nonNull)
                .filter(e -> Objects.nonNull(e.getReqCount()) && e.getReqCount() > 0L)
                .filter(e -> !filterByTime || (e.getTimeStart() >= param.getStartAt() && e.getTimeStart() < param.getEndAt()))
                .toList();
    }

    void merge(ApiMetricsRaw apiMetricsRaw, Map<String, ApiMetricsRaw> map, String key, String apiId, String serverId, QueryBase param) {
        ApiMetricsRaw itemRaw = map.computeIfAbsent(key, k -> {
            ApiMetricsRaw item = new ApiMetricsRaw();
            item.setId(new ObjectId());
            item.setApiId(apiId);
            item.setProcessId(serverId);
            item.setTimeStart(param.getStartAt());
            item.setTimeGranularity(param.getGranularity().getType());
            return item;
        });
        itemRaw.merge(apiMetricsRaw);
    }
}
