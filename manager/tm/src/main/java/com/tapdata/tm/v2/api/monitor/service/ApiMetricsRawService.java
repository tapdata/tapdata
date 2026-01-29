package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.entity.ApiCallField;
import com.tapdata.tm.base.field.BaseEntityFields;
import com.tapdata.tm.base.field.CollectionField;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.v2.api.common.main.dto.TimeRange;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.ApiMetricsRawFields;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import com.tapdata.tm.v2.api.monitor.repository.ApiMetricsRepository;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsCompressValueUtil;
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

    protected void supplementSeconds(Criteria apiCallCriteria, List<ApiMetricsRaw> supplement, List<TimeRange> ranges) {
        Criteria criteria = Criteria.where(ApiCallField.DELETE.field()).is(false);
        List<Criteria> andCriteria = new ArrayList<>();
        andCriteria.add(criteria);
        if (null != apiCallCriteria) {
            andCriteria.add(apiCallCriteria);
        }
        int rangeSize = ranges.size();
        if (1 == rangeSize) {
            TimeRange point = ranges.get(0);
            andCriteria.add(Criteria.where(ApiCallField.REQ_TIME.field()).gte(point.getStart() * 1000L).lt(point.getEnd() * 1000L));
        } else if (!ranges.isEmpty()) {
            List<Criteria> orSec = ranges.stream()
                    .map(point -> Criteria.where(ApiCallField.REQ_TIME.field()).gte(point.getStart() * 1000L).lt(point.getEnd() * 1000L))
                    .toList();
            andCriteria.add(new Criteria().orOperator(orSec));
        }
        Query query = Query.query(new Criteria().andOperator(andCriteria));
        String[] filterField = CollectionField.fields(
                ApiCallField.API_GATEWAY_UUID,
                ApiCallField.ALL_PATH_ID,
                ApiCallField.REQ_PATH,
                ApiCallField.REQ_TIME,
                ApiCallField.CODE,
                ApiCallField.HTTP_STATUS,
                ApiCallField.REQ_BYTES,
                ApiCallField.LATENCY,
                BaseEntityFields._ID,
                ApiCallField.SUCCEED
        );
        String callName = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
        query.fields().include(filterField);
        if (StringUtils.isNotBlank(callName)) {
            List<ApiCallEntity> calls = mongoTemplate.find(query, ApiCallEntity.class, callName);
            ParticleSizeAnalyzer.parseToMetric(calls, supplement::add);
        }
    }

    protected void supplementFiveSecond(Consumer<Criteria> criteriaConsumer, List<ApiMetricsRaw> supplement, List<TimeRange> ranges) {
        Criteria criteriaOfSec5 = Criteria.where(ApiMetricsRawFields.TIME_GRANULARITY.field()).is(TimeGranularity.MINUTE.getType());
        Optional.ofNullable(criteriaConsumer).ifPresent(c -> c.accept(criteriaOfSec5));
        List<Criteria> or = new ArrayList<>();
        for (TimeRange point : ranges) {
            Criteria andCriteria = new Criteria();
            List<Criteria> and = new ArrayList<>();
            and.add(Criteria.where(ApiMetricsRawFields.TIME_START.field()).gte(point.getStart()));
            and.add(Criteria.where(ApiMetricsRawFields.TIME_START.field()).lt(point.getEnd()));
            andCriteria.andOperator(and);
            or.add(andCriteria);
        }
        criteriaOfSec5.orOperator(or);
        Query query = Query.query(criteriaOfSec5);
        String[] filterFields = CollectionField.fields(
                ApiMetricsRawFields.API_ID,
                ApiMetricsRawFields.PROCESS_ID,
                ApiMetricsRawFields.TIME_GRANULARITY,
                ApiMetricsRawFields.TIME_START,
                ApiMetricsRawFields.SUB_METRICS
        );
        query.fields().include(filterFields);
        List<ApiMetricsRaw> metricsRawList = find(query);
        for (TimeRange point : ranges) {
            List<ApiMetricsRaw> raws = ParticleSizeAnalyzer.secondFiveMetricsRaws(
                    metricsRawList,
                    e -> e.getTimeStart() >= point.getStart() && e.getTimeStart() < point.getEnd());
            supplement.addAll(raws);
        }
    }

    protected void supplementMinute(Consumer<Criteria> criteriaConsumer, List<ApiMetricsRaw> supplement, List<TimeRange> ranges, String[] filterFields) {
        Criteria criteriaOfMin = Criteria.where(ApiMetricsRawFields.TIME_GRANULARITY.field()).is(TimeGranularity.HOUR.getType());
        Optional.ofNullable(criteriaConsumer).ifPresent(c -> c.accept(criteriaOfMin));
        List<Criteria> or = new ArrayList<>();
        for (TimeRange point : ranges) {
            Criteria andCriteria = new Criteria();
            List<Criteria> and = new ArrayList<>();
            and.add(Criteria.where(ApiMetricsRawFields.TIME_START.field()).gte(TimeGranularity.MINUTE.fixTime(point.getStart())));
            and.add(Criteria.where(ApiMetricsRawFields.TIME_START.field()).lt(TimeGranularity.MINUTE.fixTime(point.getEnd())));
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

    List<Criteria> eachTimeRange(List<TimeRange> currentRanges, TimeGranularity granularity) {
        List<Criteria> orTimeRange = new ArrayList<>();
        for (TimeRange range : currentRanges) {
            Criteria subRange;
            if (granularity == TimeGranularity.SECOND_FIVE) {
                subRange = Criteria.where(ApiMetricsRawFields.TIME_START.field())
                        .gte(TimeGranularity.MINUTE.fixTime(range.getStart()))
                        .lt(TimeGranularity.MINUTE.fixTime(range.getEnd() + TimeGranularity.MINUTE.getSeconds() - 1L));
            } else {
                subRange = Criteria.where(ApiMetricsRawFields.TIME_START.field()).gte(range.getStart()).lt(range.getEnd());
            }
            orTimeRange.add(subRange);
        }
        return orTimeRange;
    }

    String[] fieldByTimeGranularity(TimeGranularity granularity, String[] filterFields) {
        if (granularity == TimeGranularity.SECOND_FIVE) {
            return CollectionField.fields(
                    ApiMetricsRawFields.API_ID,
                    ApiMetricsRawFields.PROCESS_ID,
                    ApiMetricsRawFields.TIME_GRANULARITY,
                    ApiMetricsRawFields.TIME_START,
                    ApiMetricsRawFields.SUB_METRICS
            );
        }
        return filterFields;
    }

    void analyzeMetric(List<ApiMetricsRaw> result, TimeGranularity granularity, List<TimeRange> currentRanges, List<ApiMetricsRaw> apiMetricsRaws) {
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

    public List<ApiMetricsRaw> supplementMetricsRaw(QueryBase param, boolean filterByTime, Consumer<Criteria> criteriaConsumer, Criteria apiCallCriteria, String[] filterFields) {
        TimeGranularity granularity = param.getGranularity();
        Map<TimeGranularity, List<TimeRange>> queryRange = param.getQueryRange();
        List<TimeRange> currentRanges = queryRange.get(granularity);
        List<ApiMetricsRaw> result = new ArrayList<>();
        if (!CollectionUtils.isEmpty(currentRanges)) {
            TimeGranularity queryTimeGranularity = granularity == TimeGranularity.SECOND_FIVE ? TimeGranularity.MINUTE : granularity;
            Criteria criteria = Criteria.where(ApiMetricsRawFields.TIME_GRANULARITY.field()).is(queryTimeGranularity.getType());
            criteriaConsumer.accept(criteria);
            List<Criteria> orTimeRange = eachTimeRange(currentRanges, granularity);
            criteria.andOperator(orTimeRange);
            Query query = new Query(criteria);
            query.fields().include(fieldByTimeGranularity(granularity, filterFields));
            List<ApiMetricsRaw> apiMetricsRaws = find(query);
            analyzeMetric(result, granularity, currentRanges, apiMetricsRaws);
        }

        List<ApiMetricsRaw> supplement = new ArrayList<>();
        /**
         * Hour level, supplement data on the boundary that is less than 1 second, less than 1 minute, and less than 1 hour
         * Minute level, supplement data on the boundary that is less than 1 second, less than 1 minute
         * 5-second level, supplementing data on the boundary for less than 1 second
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
        eachAllApiMetricsRaw(supplement, param, left, right);
        long step = ApiMetricsCompressValueUtil.stepByGranularity(param.getGranularity());
        acceptMetric(left, result, step, 0);
        acceptMetric(right, result, step, -1);
        return result.stream()
                .filter(Objects::nonNull)
                .filter(e -> Objects.nonNull(e.getReqCount()) && e.getReqCount() > 0L)
                .filter(e -> this.judgeRaws(e, filterByTime, param))
                .toList();
    }

    void eachAllApiMetricsRaw(List<ApiMetricsRaw> supplement, QueryBase param, Map<String, ApiMetricsRaw> left, Map<String, ApiMetricsRaw> right) {
        if (CollectionUtils.isEmpty(supplement)) {
            return;
        }
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
    }

    boolean judgeRaws(ApiMetricsRaw e, boolean filterByTime, QueryBase param) {
        return !filterByTime || (e.getTimeStart() >= param.getStartAt() && e.getTimeStart() < param.getEndAt());
    }

    void acceptMetric(Map<String, ApiMetricsRaw> apiMetricsRaws, List<ApiMetricsRaw> result, long step, int addTo) {
        if (CollectionUtils.isEmpty(apiMetricsRaws)) {
            return;
        }
        apiMetricsRaws.forEach((key, item) -> Optional.ofNullable(item)
                .map(i -> {
                    i.setTimeStart(i.getTimeStart() / step * step);
                    return i;
                })
                .map(MetricInstanceAcceptor::calcPValue)
                .ifPresent(i -> {
                    if (addTo >= 0) {
                        result.add(addTo, i);
                    } else {
                        result.add(i);
                    }
                }));
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
