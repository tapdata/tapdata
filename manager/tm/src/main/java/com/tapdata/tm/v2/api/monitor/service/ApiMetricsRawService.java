package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.entity.ApiCallField;
import com.tapdata.tm.base.field.BaseEntityFields;
import com.tapdata.tm.base.field.CollectionField;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.v2.api.common.main.dto.TimeRange;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.ApiMetricsRawFields;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import com.tapdata.tm.v2.api.monitor.repository.ApiMetricsRepository;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsCompressValueUtil;
import com.mongodb.ClientSessionOptions;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
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

    public record DashboardRequestData(Map<Long, Long> requestCounts,
                                       boolean hasUnattributedData,
                                       boolean hasAttributableData) {
    }

    public DashboardRequestData dashboardRequestData(long startAt, long endAt, TimeGranularity granularity,
                                                      MetricTypes metricType, String physicalNodeId) {
        if (startAt >= endAt) {
            return new DashboardRequestData(new HashMap<>(), false, false);
        }
        ClientSessionOptions sessionOptions = ClientSessionOptions.builder().build();
        try (ClientSession session = mongoTemplate.getMongoDatabaseFactory().getSession(sessionOptions)) {
            session.startTransaction(TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).build());
            try {
                DashboardRequestData result = mongoTemplate.withSession(() -> session).execute(operations -> {
                    Map<Long, Long> counts = dashboardRequestCounts(startAt, endAt, granularity,
                            metricType, physicalNodeId, operations);
                    boolean hasUnattributedData = hasUnattributedData(startAt, endAt, operations);
                    boolean hasAttributableData = hasAttributableData(startAt, endAt, operations);
                    return new DashboardRequestData(counts, hasUnattributedData, hasAttributableData);
                });
                session.commitTransaction();
                return result;
            } catch (RuntimeException | Error e) {
                if (session.hasActiveTransaction()) {
                    session.abortTransaction();
                }
                throw e;
            }
        }
    }

    public boolean hasUnattributedData(long startAt, long endAt) {
        return hasUnattributedData(startAt, endAt, mongoTemplate);
    }

    private boolean hasUnattributedData(long startAt, long endAt, MongoOperations operations) {
        List<Criteria> overlappingBuckets = overlappingRawBuckets(startAt, endAt);
        Criteria missingNodeId = new Criteria().orOperator(
                Criteria.where(ApiMetricsRawFields.PHYSICAL_NODE_ID.field()).is(null),
                Criteria.where(ApiMetricsRawFields.PHYSICAL_NODE_ID.field()).exists(false),
                Criteria.where(ApiMetricsRawFields.PHYSICAL_NODE_ID.field()).is(""));
        Criteria rawCriteria = new Criteria().andOperator(
                Criteria.where(ApiMetricsRawFields.METRIC_TYPE.field()).is(MetricTypes.SERER.getType()),
                missingNodeId,
                new Criteria().orOperator(overlappingBuckets));
        if (operations.count(Query.query(rawCriteria), ApiMetricsRaw.class) > 0) {
            return true;
        }

        String apiCallCollection = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
        if (StringUtils.isBlank(apiCallCollection)) {
            return false;
        }
        Criteria missingCallNodeId = new Criteria().orOperator(
                Criteria.where(ApiCallField.PHYSICAL_NODE_ID.field()).is(null),
                Criteria.where(ApiCallField.PHYSICAL_NODE_ID.field()).exists(false),
                Criteria.where(ApiCallField.PHYSICAL_NODE_ID.field()).is(""));
        Criteria callCriteria = new Criteria().andOperator(
                Criteria.where(ApiCallField.DELETE.field()).is(false),
                Criteria.where(ApiCallField.API_GATEWAY_UUID.field()).exists(true).ne(null),
                missingCallNodeId,
                Criteria.where(ApiCallField.REQ_TIME.field()).gte(startAt * 1000L).lt(endAt * 1000L));
        return operations.count(Query.query(callCriteria), ApiCallEntity.class, apiCallCollection) > 0;
    }

    public boolean hasAttributableData(long startAt, long endAt) {
        return hasAttributableData(startAt, endAt, mongoTemplate);
    }

    private boolean hasAttributableData(long startAt, long endAt, MongoOperations operations) {
        if (startAt >= endAt) {
            return false;
        }
        Criteria hasNodeId = new Criteria().andOperator(
                Criteria.where(ApiMetricsRawFields.PHYSICAL_NODE_ID.field()).exists(true),
                Criteria.where(ApiMetricsRawFields.PHYSICAL_NODE_ID.field()).nin(Arrays.asList(null, "")));
        Criteria rawCriteria = new Criteria().andOperator(
                Criteria.where(ApiMetricsRawFields.METRIC_TYPE.field()).is(MetricTypes.SERER.getType()),
                hasNodeId,
                new Criteria().orOperator(overlappingRawBuckets(startAt, endAt)));
        if (operations.count(Query.query(rawCriteria), ApiMetricsRaw.class) > 0) {
            return true;
        }

        String apiCallCollection = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
        if (StringUtils.isBlank(apiCallCollection)) {
            return false;
        }
        Criteria callCriteria = new Criteria().andOperator(
                Criteria.where(ApiCallField.DELETE.field()).is(false),
                Criteria.where(ApiCallField.API_GATEWAY_UUID.field()).exists(true).ne(null),
                Criteria.where(ApiCallField.PHYSICAL_NODE_ID.field()).exists(true).nin(Arrays.asList(null, "")),
                Criteria.where(ApiCallField.REQ_TIME.field()).gte(startAt * 1000L).lt(endAt * 1000L));
        return operations.count(Query.query(callCriteria), ApiCallEntity.class, apiCallCollection) > 0;
    }

    private List<Criteria> overlappingRawBuckets(long startAt, long endAt) {
        List<Criteria> overlappingBuckets = new ArrayList<>();
        for (TimeGranularity granularity : List.of(TimeGranularity.MINUTE, TimeGranularity.HOUR)) {
            overlappingBuckets.add(new Criteria().andOperator(
                    Criteria.where(ApiMetricsRawFields.TIME_GRANULARITY.field()).is(granularity.getType()),
                    Criteria.where(ApiMetricsRawFields.TIME_START.field()).gt(startAt - granularity.getSeconds()).lt(endAt)));
        }
        return overlappingBuckets;
    }

    public Map<Long, Long> dashboardRequestCounts(long startAt, long endAt, TimeGranularity granularity,
                                                  MetricTypes metricType, String physicalNodeId) {
        return dashboardRequestData(startAt, endAt, granularity, metricType, physicalNodeId).requestCounts();
    }

    private Map<Long, Long> dashboardRequestCounts(long startAt, long endAt, TimeGranularity granularity,
                                                   MetricTypes metricType, String physicalNodeId,
                                                   MongoOperations operations) {
        Map<Long, Long> counts = new HashMap<>();
        if (startAt >= endAt) {
            return counts;
        }

        if (granularity == TimeGranularity.HOUR) {
            addHourlyRange(counts, granularity, startAt, endAt, metricType, physicalNodeId, operations);
        } else if (granularity == TimeGranularity.MINUTE) {
            addMinuteRange(counts, granularity, startAt, endAt, metricType, physicalNodeId, operations);
        } else {
            addFiveSecondRange(counts, granularity, startAt, endAt, metricType, physicalNodeId, operations);
        }
        return counts;
    }

    private void addHourlyRange(Map<Long, Long> counts, TimeGranularity outputGranularity,
                                long startAt, long endAt, MetricTypes metricType, String physicalNodeId,
                                MongoOperations operations) {
        long hourSeconds = TimeGranularity.HOUR.getSeconds();
        long fullStart = ceilToInterval(startAt, hourSeconds);
        long fullEnd = endAt - endAt % hourSeconds;
        if (fullStart >= fullEnd) {
            addMinuteWindow(counts, outputGranularity, startAt, endAt, metricType, physicalNodeId, operations);
            return;
        }
        addMinuteWindow(counts, outputGranularity, startAt, fullStart, metricType, physicalNodeId, operations);
        addRawCountRange(counts, outputGranularity, TimeGranularity.HOUR, fullStart, fullEnd, metricType, physicalNodeId, operations);
        addMinuteWindow(counts, outputGranularity, fullEnd, endAt, metricType, physicalNodeId, operations);
    }

    private void addMinuteRange(Map<Long, Long> counts, TimeGranularity outputGranularity,
                                long startAt, long endAt, MetricTypes metricType, String physicalNodeId,
                                MongoOperations operations) {
        long minuteSeconds = TimeGranularity.MINUTE.getSeconds();
        long fullStart = ceilToInterval(startAt, minuteSeconds);
        long fullEnd = endAt - endAt % minuteSeconds;
        if (fullStart >= fullEnd) {
            addApiCallRange(counts, outputGranularity, startAt, endAt, physicalNodeId, operations);
            return;
        }
        addApiCallRange(counts, outputGranularity, startAt, fullStart, physicalNodeId, operations);
        addRawCountRange(counts, outputGranularity, TimeGranularity.MINUTE, fullStart, fullEnd, metricType, physicalNodeId, operations);
        addApiCallRange(counts, outputGranularity, fullEnd, endAt, physicalNodeId, operations);
    }

    private void addMinuteWindow(Map<Long, Long> counts, TimeGranularity outputGranularity,
                                 long startAt, long endAt, MetricTypes metricType, String physicalNodeId,
                                 MongoOperations operations) {
        addMinuteRange(counts, outputGranularity, startAt, endAt, metricType, physicalNodeId, operations);
    }

    private void addFiveSecondRange(Map<Long, Long> counts, TimeGranularity outputGranularity,
                                    long startAt, long endAt, MetricTypes metricType, String physicalNodeId,
                                    MongoOperations operations) {
        long fiveSeconds = TimeGranularity.SECOND_FIVE.getSeconds();
        long fullStart = ceilToInterval(startAt, fiveSeconds);
        long fullEnd = endAt - endAt % fiveSeconds;
        if (fullStart >= fullEnd) {
            addApiCallRange(counts, outputGranularity, startAt, endAt, physicalNodeId, operations);
            return;
        }
        addApiCallRange(counts, outputGranularity, startAt, fullStart, physicalNodeId, operations);

        long parentStart = TimeGranularity.MINUTE.fixTime(fullStart);
        long parentEnd = TimeGranularity.MINUTE.fixTime(fullEnd - 1L) + TimeGranularity.MINUTE.getSeconds();
        Criteria criteria = dashboardRawCriteria(TimeGranularity.MINUTE, parentStart, parentEnd, metricType, physicalNodeId);
        Query query = new Query(criteria);
        query.fields().include(ApiMetricsRawFields.API_ID.field(), ApiMetricsRawFields.REQ_PATH.field(),
                ApiMetricsRawFields.PROCESS_ID.field(), ApiMetricsRawFields.PHYSICAL_NODE_ID.field(),
                ApiMetricsRawFields.TIME_GRANULARITY.field(), ApiMetricsRawFields.TIME_START.field(),
                ApiMetricsRawFields.SUB_METRICS.field());
        for (ApiMetricsRaw minute : operations.find(query, ApiMetricsRaw.class)) {
            if (CollectionUtils.isEmpty(minute.getSubMetrics())) {
                continue;
            }
            for (ApiMetricsRaw sample : minute.getSubMetrics().values()) {
                if (sample.getTimeStart() != null && sample.getTimeStart() >= fullStart && sample.getTimeStart() < fullEnd) {
                    mergeRequestCount(counts, outputGranularity, sample.getTimeStart(), sample.getReqCount());
                }
            }
        }
        addPendingApiCallRange(counts, outputGranularity, fullStart, fullEnd, physicalNodeId, operations);
        addApiCallRange(counts, outputGranularity, fullEnd, endAt, physicalNodeId, operations);
    }

    private void addRawCountRange(Map<Long, Long> counts, TimeGranularity outputGranularity,
                                  TimeGranularity sourceGranularity, long startAt, long endAt,
                                  MetricTypes metricType, String physicalNodeId, MongoOperations operations) {
        if (startAt >= endAt) {
            return;
        }
        Query query = new Query(dashboardRawCriteria(sourceGranularity, startAt, endAt, metricType, physicalNodeId));
        query.fields().include(ApiMetricsRawFields.TIME_START.field(), ApiMetricsRawFields.REQ_COUNT.field());
        for (ApiMetricsRaw row : operations.find(query, ApiMetricsRaw.class)) {
            if (row.getTimeStart() != null) {
                mergeRequestCount(counts, outputGranularity, row.getTimeStart(), row.getReqCount());
            }
        }
        addPendingApiCallRange(counts, outputGranularity, startAt, endAt, physicalNodeId, operations);
    }

    private Criteria dashboardRawCriteria(TimeGranularity sourceGranularity, long startAt, long endAt,
                                          MetricTypes metricType, String physicalNodeId) {
        Criteria criteria = Criteria.where(ApiMetricsRawFields.METRIC_TYPE.field()).is(metricType.getType())
                .and(ApiMetricsRawFields.TIME_GRANULARITY.field()).is(sourceGranularity.getType())
                .and(ApiMetricsRawFields.TIME_START.field()).gte(startAt).lt(endAt);
        if (StringUtils.isNotBlank(physicalNodeId)) {
            criteria.and(ApiMetricsRawFields.PHYSICAL_NODE_ID.field()).is(physicalNodeId);
        }
        return criteria;
    }

    private void addApiCallRange(Map<Long, Long> counts, TimeGranularity outputGranularity,
                                 long startAt, long endAt, String physicalNodeId, MongoOperations operations) {
        if (startAt >= endAt) {
            return;
        }
        String apiCallCollection = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
        if (StringUtils.isBlank(apiCallCollection)) {
            return;
        }
        Criteria criteria = Criteria.where(ApiCallField.DELETE.field()).is(false)
                .and(ApiCallField.API_GATEWAY_UUID.field()).exists(true).ne(null)
                .and(ApiCallField.REQ_TIME.field()).gte(startAt * 1000L).lt(endAt * 1000L);
        if (StringUtils.isNotBlank(physicalNodeId)) {
            criteria.and(ApiCallField.PHYSICAL_NODE_ID.field()).is(physicalNodeId);
        }
        Query query = Query.query(criteria);
        query.fields().include(ApiCallField.REQ_TIME.field());
        query.cursorBatchSize(1000);
        try (var calls = operations.stream(query, ApiCallEntity.class, apiCallCollection)) {
            calls.filter(Objects::nonNull)
                    .map(ApiCallEntity::getReqTime)
                    .filter(Objects::nonNull)
                    .forEach(reqTime -> {
                        long requestAt = reqTime / 1000L;
                        long bucket = requestAt - requestAt % outputGranularity.getSeconds();
                        counts.merge(bucket, 1L, Long::sum);
                    });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Unable to stream API calls for dashboard trend", e);
        }
    }

    private void addPendingApiCallRange(Map<Long, Long> counts, TimeGranularity outputGranularity,
                                        long startAt, long endAt, String physicalNodeId, MongoOperations operations) {
        if (startAt >= endAt) {
            return;
        }
        String apiCallCollection = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
        if (StringUtils.isBlank(apiCallCollection)) {
            return;
        }
        Criteria criteria = Criteria.where(ApiCallField.DELETE.field()).is(false)
                .and(ApiCallField.HAS_METRIC.field()).is(false)
                .and(ApiCallField.API_GATEWAY_UUID.field()).exists(true).ne(null)
                .and(ApiCallField.REQ_TIME.field()).gte(startAt * 1000L).lt(endAt * 1000L);
        if (StringUtils.isNotBlank(physicalNodeId)) {
            criteria.and(ApiCallField.PHYSICAL_NODE_ID.field()).is(physicalNodeId);
        }
        Query query = Query.query(criteria);
        query.fields().include(ApiCallField.REQ_TIME.field());
        query.cursorBatchSize(1000);
        try (var calls = operations.stream(query, ApiCallEntity.class, apiCallCollection)) {
            calls.filter(Objects::nonNull)
                    .map(ApiCallEntity::getReqTime)
                    .filter(Objects::nonNull)
                    .forEach(reqTime -> {
                        long requestAt = reqTime / 1000L;
                        long bucket = requestAt - requestAt % outputGranularity.getSeconds();
                        counts.merge(bucket, 1L, Long::sum);
                    });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Unable to stream pending API calls for dashboard trend", e);
        }
    }

    private void mergeRequestCount(Map<Long, Long> counts, TimeGranularity outputGranularity, Long timestamp, Long requestCount) {
        if (requestCount == null) {
            return;
        }
        long bucket = timestamp - timestamp % outputGranularity.getSeconds();
        counts.merge(bucket, requestCount, Long::sum);
    }

    private long ceilToInterval(long value, long interval) {
        long remainder = value % interval;
        return remainder == 0 ? value : value + interval - remainder;
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
                ApiCallField.PHYSICAL_NODE_ID,
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
                ApiMetricsRawFields.REQ_PATH,
                ApiMetricsRawFields.PROCESS_ID,
                ApiMetricsRawFields.PHYSICAL_NODE_ID,
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
                    ApiMetricsRawFields.REQ_PATH,
                    ApiMetricsRawFields.PROCESS_ID,
                ApiMetricsRawFields.PHYSICAL_NODE_ID,
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
            String key = String.format("%s_%s_%s", serverId, apiId, param.getPhysicalNodeId());
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
            item.setPhysicalNodeId(param.getPhysicalNodeId());
            item.setTimeStart(param.getStartAt());
            item.setTimeGranularity(param.getGranularity().getType());
            return item;
        });
        itemRaw.merge(apiMetricsRaw);
    }
}
