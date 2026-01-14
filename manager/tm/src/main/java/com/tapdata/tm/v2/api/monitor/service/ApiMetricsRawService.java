package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.service.WorkerCallServiceImpl;
import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.apiServer.utils.PercentileCalculator;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
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

import java.util.ArrayList;
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


    public List<ApiMetricsRaw> find(Query query) {
        return repository.findAll(query);
    }

    public List<WorkerCallEntity> supplementWorkerCall(TopWorkerInServerParam param) {
        String serverId = param.getServerId();
        long endAt = param.getEndAt() * 1000L;
        if (endAt % 60000L != 0L) {
            endAt = (endAt + 60000L) / 60000L * 60000L;
        }
        Criteria criteriaOfWorker = Criteria.where(WorkerCallServiceImpl.Tag.TIME_GRANULARITY).is(1)
                .and(WorkerCallServiceImpl.Tag.DELETE).ne(true)
                .and(WorkerCallServiceImpl.Tag.PROCESS_ID).is(serverId)
                .andOperator(
                        Criteria.where(WorkerCallServiceImpl.Tag.TIME_START).gte(param.getStartAt() * 1000L),
                        Criteria.where(WorkerCallServiceImpl.Tag.TIME_START).lt(endAt)
                );
        List<WorkerCallEntity> callOfWorker = mongoTemplate.find(Query.query(criteriaOfWorker), WorkerCallEntity.class, "ApiCallInWorker");
        List<QueryBase.Point> secondPoint = new ArrayList<>();
        Long s = param.getStartAt();
        Long e = param.getEndAt();
        Long start = param.getQueryParam().getStart();
        Long end = param.getQueryParam().getStart();
        if (s < start) {
            secondPoint.add(QueryBase.Point.of(s, start, -1));
        }
        if (end > e) {
            secondPoint.add(QueryBase.Point.of(e, end, -1));
        }
        if (!secondPoint.isEmpty()) {
            Criteria criteriaOfSec = Criteria.where("delete").ne(true)
                    .and("req_path").nin(MetricInstanceFactory.IGNORE_PATH);
            List<Criteria> or = new ArrayList<>();
            for (QueryBase.Point point : secondPoint) {
                Criteria andCriteria = new Criteria();
                List<Criteria> and = new ArrayList<>();
                and.add(Criteria.where("reqTime").gte(point.getStart() * 1000L));
                and.add(Criteria.where("reqTime").lt(point.getEnd() * 1000L));
                andCriteria.andOperator(and);
                or.add(andCriteria);
            }
            criteriaOfSec.orOperator(or);
            Query query = Query.query(criteriaOfSec);
            String callName = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
            if (StringUtils.isNotBlank(callName)) {
                List<ApiCallEntity> calls = mongoTemplate.find(query, ApiCallEntity.class, callName);
                Map<String, List<ApiCallEntity>> collect = calls.stream()
                        .filter(Objects::nonNull)
                        .filter(callItem -> StringUtils.isNotBlank(callItem.getWorkOid()))
                        .collect(Collectors.groupingBy(ApiCallEntity::getWorkOid));
                collect.forEach((oId, callItems) -> {
                    WorkerCallEntity left = one(serverId, oId, start);
                    WorkerCallEntity right = one(serverId, oId, e);
                    for (ApiCallEntity call : callItems) {
                        Long reqTime = call.getReqTime();
                        if (null == reqTime) {
                            continue;
                        }
                        if (reqTime >= s && reqTime < start) {
                            merge(left, call);
                        } else if (reqTime >= e && reqTime < end) {
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
        Long p50L = PercentileCalculator.calculatePercentile(one.getDelays(), 0.5d);
        Long p95L = PercentileCalculator.calculatePercentile(one.getDelays(), 0.95d);
        Long p99L = PercentileCalculator.calculatePercentile(one.getDelays(), 0.99d);
        one.setP50(p50L);
        one.setP95(p95L);
        one.setP99(p99L);
    }

    void merge(WorkerCallEntity one, ApiCallEntity entity) {
        one.setReqCount(one.getReqCount() + 1L);
        one.setRps(one.getReqCount() / 60D);
        one.getDelays().add(entity.getLatency());
        boolean isOk = ApiMetricsDelayInfoUtil.checkByCode(entity.getCode(), entity.getHttpStatus());
        one.setErrorCount(one.getErrorCount() + (isOk ? 0L : 1L));
        one.setErrorRate(1D * one.getErrorCount() / one.getReqCount());
    }

    WorkerCallEntity one(String serverId, String oId, Long time) {
        WorkerCallEntity one = new WorkerCallEntity() ;
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

    public List<ApiMetricsRaw> supplementMetricsRaw(List<ApiMetricsRaw> apiMetricsRaws, QueryBase param, Consumer<Criteria> criteriaConsumer, Consumer<Criteria> apiCallCriteriaConsumer) {
        List<ApiMetricsRaw> result = ParticleSizeAnalyzer.apiMetricsRaws(apiMetricsRaws, param);
        List<ApiMetricsRaw> supplement = new ArrayList<>();
        int granularity = param.getGranularity();
        switch (granularity) {
            case 0:
                List<QueryBase.Point> secondPoint = param.getQueryParam().getSecondPoint();
                if (!secondPoint.isEmpty()) {
                    Criteria criteriaOfSec = Criteria.where("delete").ne(true)
                            .and("req_path").nin(MetricInstanceFactory.IGNORE_PATH);
                    apiCallCriteriaConsumer.accept(criteriaOfSec);
                    List<Criteria> or = new ArrayList<>();
                    for (QueryBase.Point point : secondPoint) {
                        Criteria andCriteria = new Criteria();
                        List<Criteria> and = new ArrayList<>();
                        and.add(Criteria.where("reqTime").gte(point.getStart() * 1000L));
                        and.add(Criteria.where("reqTime").lt(point.getEnd() * 1000L));
                        andCriteria.andOperator(and);
                        or.add(andCriteria);
                    }
                    criteriaOfSec.orOperator(or);
                    Query query = Query.query(criteriaOfSec);
                    String callName = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
                    if (StringUtils.isNotBlank(callName)) {
                        List<ApiCallEntity> calls = mongoTemplate.find(query, ApiCallEntity.class, callName);
                        ParticleSizeAnalyzer.parseToMetric(calls, supplement::add);
                    }
                }
            case 1:
                List<QueryBase.Point> second5Point = param.getQueryParam().getSecond5Point();
                if (!second5Point.isEmpty()) {
                    Criteria criteriaOfSec5 = Criteria.where("delete").ne(true)
                            .and("timeGranularity").is(1);
                    criteriaConsumer.accept(criteriaOfSec5);
                    List<Criteria> or = new ArrayList<>();
                    for (QueryBase.Point point : second5Point) {
                        Criteria andCriteria = new Criteria();
                        List<Criteria> and = new ArrayList<>();
                        and.add(Criteria.where("timeStart").gte(point.getStart()));
                        and.add(Criteria.where("timeStart").lt(point.getEnd()));
                        andCriteria.andOperator(and);
                        or.add(andCriteria);
                    }
                    criteriaOfSec5.orOperator(or);
                    Query query = Query.query(criteriaOfSec5);
                    QueryBase of5Sec = new QueryBase();
                    of5Sec.setGranularity(0);
                    List<ApiMetricsRaw> metricsRawList = find(query);
                    for (QueryBase.Point point : second5Point) {
                        of5Sec.setStartAt(point.getStart());
                        of5Sec.setEndAt(point.getEnd());
                        List<ApiMetricsRaw> raws = ParticleSizeAnalyzer.apiMetricsRaws(metricsRawList, of5Sec);
                        supplement.addAll(raws);
                    }
                }
            case 2:
                List<QueryBase.Point> minutePoint = param.getQueryParam().getMinutePoint();
                if (!minutePoint.isEmpty()) {
                    Criteria criteriaOfMin = Criteria.where("delete").ne(true)
                            .and("timeGranularity").is(2);
                    criteriaConsumer.accept(criteriaOfMin);
                    List<Criteria> or = new ArrayList<>();
                    for (QueryBase.Point point : minutePoint) {
                        Criteria andCriteria = new Criteria();
                        List<Criteria> and = new ArrayList<>();
                        and.add(Criteria.where("timeStart").gte((point.getStart() / 60L * 60L)));
                        and.add(Criteria.where("timeStart").lt(point.getEnd() / 60L * 60L));
                        andCriteria.andOperator(and);
                        or.add(andCriteria);
                    }
                    criteriaOfMin.orOperator(or);
                    Query query = Query.query(criteriaOfMin);
                    List<ApiMetricsRaw> raws = find(query);
                    if (!raws.isEmpty()) {
                        supplement.addAll(raws);
                    }
                }
                break;
            default:
                //do nothing
        }
        ApiMetricsRaw left = null;
        ApiMetricsRaw right = null;
        for (ApiMetricsRaw apiMetricsRaw : supplement) {
            Long timeStart = apiMetricsRaw.getTimeStart();
            if (null == timeStart) {
                continue;
            }
            if (timeStart >= param.getQueryParam().getStart() && timeStart < param.getStartAt()) {
                if (null == left) {
                    left = new ApiMetricsRaw();
                    left.setTimeStart(param.getQueryParam().getStart());
                    left.setTimeGranularity(param.getGranularity());
                }
                left.merge(apiMetricsRaw);
            }
            if (timeStart >= param.getStartAt() && timeStart < param.getQueryParam().getEnd()) {
                if (null == left) {
                    right = new ApiMetricsRaw();
                    right.setTimeStart(param.getStartAt());
                    right.setTimeGranularity(param.getGranularity());
                }
                right.merge(apiMetricsRaw);
            }
        }
        long step = switch (param.getGranularity()) {
            case 1 -> 60L;
            case 2 -> 3600L;
            default -> 5L;
        };
        Optional.ofNullable(left)
                .map(e -> {e.setTimeStart(e.getTimeStart() / step * step); return e;})
                .map(MetricInstanceAcceptor::calcPValue)
                .ifPresent(e -> result.add(0, e));
        Optional.ofNullable(right)
                .map(e -> {e.setTimeStart(e.getTimeStart() / step * step); return e;})
                .map(MetricInstanceAcceptor::calcPValue)
                .ifPresent(result::add);
        return result;
    }

}
