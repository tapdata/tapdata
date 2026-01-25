package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.constant.ModuleStatusEnum;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.v2.api.common.main.dto.TimeRange;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiItem;
import com.tapdata.tm.v2.api.monitor.main.dto.DataValueBase;
import com.tapdata.tm.v2.api.monitor.main.dto.TopApiInServer;
import com.tapdata.tm.v2.api.monitor.main.dto.ValueBase;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.main.param.ApiListParam;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import com.tapdata.tm.v2.api.monitor.main.param.TopApiInServerParam;
import com.tapdata.tm.v2.api.monitor.repository.ApiMetricsRepository;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsDelayInfoUtil;
import com.tapdata.tm.v2.api.monitor.utils.ApiPathUtil;
import com.tapdata.tm.v2.api.monitor.utils.ChartSortUtil;
import com.tapdata.tm.v2.api.monitor.utils.TimeRangeUtil;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/21 00:36 Create
 * @description
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class ApiMetricsRawMergeService {
    private MongoTemplate mongoTemplate;
    ApiMetricsRawService service;
    ModulesService modulesService;
    SettingsService settingsService;
    private ApiMetricsRepository apiMetricsRepository;

    public long getDelay() {
        Settings byKey = settingsService.getByKey("apiStatsBatchReport.timeSpanOfTriggerApiStatsBatchReport");
        if (null == byKey) {
            return 15000L;
        }
        long delay = 15000L;
        Object value = byKey.getValue();
        if (value instanceof Number iValue) {
            delay = iValue.longValue();
        } else if (value instanceof String iValue) {
            try {
                delay = Long.parseLong(StringUtils.trim(iValue));
            } catch (NumberFormatException e) {
                //do nothing
            }
        }
        if (byKey.getDefault_value() instanceof Number iValue) {
            delay = iValue.longValue();
        } else if (byKey.getDefault_value() instanceof String iValue) {
            try {
                delay = Long.parseLong(StringUtils.trim(iValue));
            } catch (NumberFormatException e) {
                //do nothing
            }
        }
        return Math.min(Math.max(delay + 10000L, 15000L), 90000L);
    }

    protected TopApiInServer groupAsTopApiInServer(List<ApiMetricsRaw> rows, TopApiInServerParam param) {
        TopApiInServer item = TopApiInServer.create();
        item.setQueryFrom(param.getStartAt());
        item.setQueryEnd(param.getEndAt());
        item.setGranularity(param.getGranularity().getType());
        long errorCount = errorCountGetter(rows, e -> item.setRequestCount(item.getRequestCount() + e));
        int total = item.getRequestCount().intValue();
        if (total > 0) {
            item.setErrorRate(ApiMetricsDelayInfoUtil.rate(errorCount, item.getRequestCount()));
            item.setErrorCount(errorCount);
            baseDataCalculate(item, rows, null);
        }
        return item;
    }

    public Page<TopApiInServer> topApiInServer(TopApiInServerParam param) {
        String serverId = param.getServerId();
        if (StringUtils.isBlank(serverId)) {
            throw new BizException(ApiMetricsChartQuery.SERVER_ID_EMPTY);
        }
        long delay = getDelay();
        TimeRangeUtil.rangeOf(param, delay, true);
        List<ApiMetricsRaw> apiMetricsRaws = merge(
                param,
                c -> c.and(ApiMetricsChartQuery.PROCESS_ID).is(serverId).and("metricType").is(MetricTypes.API_SERVER.getType()),
                Criteria.where("api_gateway_uuid").is(serverId),
                new String[]{"apiId", "processId", "timeGranularity", "timeStart", "reqCount", "errorCount", "delay"});
        Map<String, TopApiInServer> apiInfoMap = apiMetricsRaws.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getApiId()))
                .collect(
                        Collectors.groupingBy(
                                ApiMetricsRaw::getApiId,
                                Collectors.collectingAndThen(
                                        Collectors.toList(),
                                        rows -> this.groupAsTopApiInServer(rows, param)
                                )
                        ));
        List<ObjectId> apiIds = apiMetricsRaws.stream()
                .map(ApiMetricsRaw::getApiId)
                .distinct()
                .map(MongoUtils::toObjectId)
                .filter(Objects::nonNull)
                .toList();
        apiInfoMap.forEach(this::initApiInfo);
        if (CollectionUtils.isEmpty(apiIds)) {
            List<TopApiInServer> api = TopApiInServer.supplement(new ArrayList<>(), publishApis(), e -> TopApiInServer.create());
            return Page.page(api.stream().skip(param.getSkip()).limit(param.getLimit()).toList(), api.size());
        }
        Criteria criteriaOfApi = Criteria.where("_id").in(apiIds);
        Query queryOfApi = Query.query(criteriaOfApi);
        List<ModulesDto> apiDtoList = modulesService.findAll(queryOfApi);
        mapApiInfo(apiDtoList, apiInfoMap, k -> TopApiInServer.create());
        List<TopApiInServer> result = new ArrayList<>(apiInfoMap.values());
        TopApiInServer.supplement(result, publishApis(), e -> TopApiInServer.create());
        ChartSortUtil.sort(result, param.getSortInfo(), TopApiInServer.class);
        return Page.page(result.stream().skip(param.getSkip()).limit(param.getLimit()).toList(), result.size());
    }

    protected <T extends TopApiInServer> void initApiInfo(String apiId, T apiInfo) {
        apiInfo.setApiId(apiId);
        apiInfo.setApiName(apiId);
        apiInfo.setApiPath(apiId);
        apiInfo.setNotExistsApi(true);
    }

    protected <T extends TopApiInServer> void mapApiInfo(List<ModulesDto> apiDtoList, Map<String, T> apiInfoMap, Function<String, T> instance) {
        if (CollectionUtils.isEmpty(apiDtoList)) {
            return;
        }
        apiDtoList.forEach(apiDto -> {
            String apiId = apiDto.getId().toHexString();
            T item = apiInfoMap.computeIfAbsent(apiId, instance);
            String path = ApiPathUtil.apiPath(apiDto.getApiVersion(), apiDto.getBasePath(), apiDto.getPrefix());
            item.setApiId(apiId);
            item.setApiName(apiDto.getName());
            item.setApiPath(path);
            item.setNotExistsApi(Optional.ofNullable(apiDto.getIsDeleted()).orElse(false));
        });
    }


    protected ApiItem groupAsApiItem(List<ApiMetricsRaw> rows, ApiListParam param) {
        ApiItem item = ApiItem.create();
        item.setQueryFrom(param.getStartAt());
        item.setQueryEnd(param.getEndAt());
        item.setGranularity(param.getGranularity().getType());
        long errorCount = errorCountGetter(rows, e -> item.setRequestCount(item.getRequestCount() + e));
        int total = item.getRequestCount().intValue();
        if (total > 0) {
            item.setErrorRate(ApiMetricsDelayInfoUtil.rate(errorCount, item.getRequestCount()));
            item.setErrorCount(errorCount);
            baseDataCalculate(item, rows, null);
            long sumRps = rows.stream()
                    .filter(Objects::nonNull)
                    .map(ApiMetricsRaw::getBytes)
                    .map(ApiMetricsDelayUtil::sum)
                    .map(ApiMetricsDelayUtil.Sum::getTotal)
                    .mapToLong(Long::longValue)
                    .sum();
            baseDataCalculate(item, rows, sumDelay -> item.setTotalRps(sumDelay > 0 ? 1000.0D * sumRps / sumDelay : 0D));
        }
        return item;
    }

    public Page<ApiItem> apiOverviewList(ApiListParam param) {
        long delay = getDelay();
        TimeRangeUtil.rangeOf(param, delay, true);
        List<ApiMetricsRaw> apiMetricsRaws = merge(
                param,
                c -> c.and("metricType").is(MetricTypes.API.getType()),
                null,
                new String[]{"apiId", "processId", "timeGranularity", "timeStart", "reqCount", "errorCount", "delay"}
        );
        Map<String, ApiItem> apiInfoMap = apiMetricsRaws.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getApiId()))
                .collect(
                        Collectors.groupingBy(
                                ApiMetricsRaw::getApiId,
                                Collectors.collectingAndThen(
                                        Collectors.toList(),
                                        rows -> this.groupAsApiItem(rows, param)
                                )
                        ));
        List<ObjectId> apiIds = apiMetricsRaws.stream()
                .map(ApiMetricsRaw::getApiId)
                .distinct()
                .map(MongoUtils::toObjectId)
                .filter(Objects::nonNull)
                .toList();
        apiInfoMap.forEach(this::initApiInfo);
        if (CollectionUtils.isEmpty(apiIds)) {
            List<ApiItem> api = TopApiInServer.supplement(new ArrayList<>(), publishApis(), e -> ApiItem.create());
            return Page.page(api.stream().skip(param.getSkip()).limit(param.getLimit()).toList(), api.size());
        }
        Criteria criteriaOfApi = Criteria.where("_id").in(apiIds);
        Query queryOfApi = Query.query(criteriaOfApi);
        List<ModulesDto> apiDtoList = modulesService.findAll(queryOfApi);
        mapApiInfo(apiDtoList, apiInfoMap, k -> ApiItem.create());
        List<ApiItem> result = new ArrayList<>(apiInfoMap.values());
        TopApiInServer.supplement(result, publishApis(), e -> ApiItem.create());
        ChartSortUtil.sort(result, param.getSortInfo(), ApiItem.class);
        return Page.page(result.stream().skip(param.getSkip()).limit(param.getLimit()).toList(), result.size());
    }

    protected void mergeOfSecondRange(List<ApiMetricsRaw> metricsRawList, List<TimeRange> ranges, Criteria apiCallCriteria) {
        List<Criteria> andCriteria = new ArrayList<>();
        andCriteria.add(Criteria.where("delete").is(false));
        Optional.ofNullable(apiCallCriteria).ifPresent(andCriteria::add);
        List<Criteria> orSec = new ArrayList<>();
        for (TimeRange point : ranges) {
            orSec.add(Criteria.where("reqTime").gte(point.getStart() * 1000L).lt(point.getEnd() * 1000L));
        }
        andCriteria.add(new Criteria().orOperator(orSec));
        Query query = Query.query(new Criteria().andOperator(andCriteria));
        query.fields().include("api_gateway_uuid", "allPathId", "req_path", "reqTime", "code", "httpStatus", "req_bytes", "latency", "_id", "workOid");
        String callName = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
        if (StringUtils.isNotBlank(callName)) {
            List<ApiCallEntity> calls = mongoTemplate.find(query, ApiCallEntity.class, callName);
            ParticleSizeAnalyzer.parseToMetric(calls, metricsRawList::add);
        }
    }

    protected void mergeOfSecondFiveRange(List<ApiMetricsRaw> metricsRawList, List<TimeRange> ranges, Consumer<Criteria> criteriaConsumer) {
        Criteria criteriaOfSec5 = Criteria.where("timeGranularity").is(1);
        Optional.ofNullable(criteriaConsumer).ifPresent(c -> c.accept(criteriaOfSec5));
        Set<Long> times = ParticleSizeAnalyzer.asMinute(ranges);
        if (times.isEmpty()) {
            return;
        }
        criteriaOfSec5.and("timeStart").in(times);
        Query querySec5 = Query.query(criteriaOfSec5);
        querySec5.fields().include("apiId", "processId", "timeGranularity", "timeStart", "subMetrics", "workerInfoMap");
        List<ApiMetricsRaw> metricsRawListSec5 = service.find(querySec5);
        for (TimeRange point : ranges) {
            List<ApiMetricsRaw> raws = ParticleSizeAnalyzer.secondFiveMetricsRaws(
                    metricsRawListSec5,
                    e -> e.getTimeStart() >= point.getStart() && e.getTimeStart() < point.getEnd()
            );
            if (!raws.isEmpty()) {
                metricsRawList.addAll(raws);
            }
        }
    }

    protected void mergeOfMinuteRange(List<ApiMetricsRaw> metricsRawList, List<TimeRange> ranges, Consumer<Criteria> criteriaConsumer, String[] filterFields) {
        Criteria criteriaMin = Criteria.where("timeGranularity").is(1);
        Optional.ofNullable(criteriaConsumer).ifPresent(c -> c.accept(criteriaMin));
        List<Criteria> or = new ArrayList<>();
        for (TimeRange point : ranges) {
            or.add(Criteria.where("timeStart").gte(point.getStart()).lt(point.getEnd()));
        }
        criteriaMin.orOperator(or);
        Query queryMin = Query.query(criteriaMin);
        queryMin.fields().include(filterFields);
        List<ApiMetricsRaw> metricsRawListMin = service.find(queryMin);
        metricsRawList.addAll(metricsRawListMin);
    }

    protected void mergeOfHour(List<ApiMetricsRaw> metricsRawList, List<TimeRange> ranges, Consumer<Criteria> criteriaConsumer, String[] filterFields) {
        Criteria criteriaOfHour = Criteria.where("timeGranularity").is(2);
        Optional.ofNullable(criteriaConsumer).ifPresent(c -> c.accept(criteriaOfHour));
        List<Criteria> orHour = new ArrayList<>();
        for (TimeRange point : ranges) {
            orHour.add(Criteria.where("timeStart").gte(point.getStart()).lt(point.getEnd()));
        }
        criteriaOfHour.orOperator(orHour);
        Query queryHour = Query.query(criteriaOfHour);
        queryHour.fields().include(filterFields);
        List<ApiMetricsRaw> metricsRawListHour = service.find(queryHour);
        metricsRawList.addAll(metricsRawListHour);
    }

    protected void mergeDay(List<ApiMetricsRaw> metricsRawList, List<TimeRange> ranges, Consumer<Criteria> criteriaConsumer, String[] filterFields) {
        Criteria criteriaOfDay = Criteria.where("timeGranularity").is(TimeGranularity.DAY.getType());
        Optional.ofNullable(criteriaConsumer).ifPresent(c -> c.accept(criteriaOfDay));
        List<Criteria> orDay = new ArrayList<>();
        for (TimeRange point : ranges) {
            orDay.add(Criteria.where("timeStart").gte(point.getStart()).lt(point.getEnd()));
        }
        criteriaOfDay.orOperator(orDay);
        Query queryDay = Query.query(criteriaOfDay);
        queryDay.fields().include(filterFields);
        List<ApiMetricsRaw> metricsRawListDay = service.find(queryDay);
        metricsRawList.addAll(metricsRawListDay);
    }

    public List<ApiMetricsRaw> merge(QueryBase param, Consumer<Criteria> criteriaConsumer, Criteria apiCallCriteria, String[] filterFields) {
        List<ApiMetricsRaw> metricsRawList = new ArrayList<>();
        Map<TimeGranularity, List<TimeRange>> queryRange = param.getQueryRange();
        queryRange.forEach((timeGranularity, ranges) -> {
            if (ranges.isEmpty()) {
                return;
            }
            switch (timeGranularity) {
                case SECOND:
                    mergeOfSecondRange(metricsRawList, ranges, apiCallCriteria);
                    break;
                case SECOND_FIVE:
                    mergeOfSecondFiveRange(metricsRawList, ranges, criteriaConsumer);
                    break;
                case MINUTE:
                    mergeOfMinuteRange(metricsRawList, ranges, criteriaConsumer, filterFields);
                    break;
                case HOUR:
                    mergeOfHour(metricsRawList, ranges, criteriaConsumer, filterFields);
                    break;
                default:
                    mergeDay(metricsRawList, ranges, criteriaConsumer, filterFields);
            }
        });
        return metricsRawList;
    }

    protected Collection<String> errorCountOfSecondRange(List<TimeRange> ranges, Criteria apiCallCriteria) {
        List<Criteria> andCriteria = new ArrayList<>();
        andCriteria.add(Criteria.where("delete").is(false));
        Optional.ofNullable(apiCallCriteria).ifPresent(andCriteria::add);
        List<Criteria> orSec = new ArrayList<>();
        for (TimeRange point : ranges) {
            orSec.add(Criteria.where("reqTime").gte(point.getStart() * 1000L).lt(point.getEnd() * 1000L));
        }
        andCriteria.add(new Criteria().orOperator(orSec));
        Query query = Query.query(new Criteria().andOperator(andCriteria));
        query.fields().include("allPathId", "code", "httpStatus");
        String callName = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
        if (StringUtils.isNotBlank(callName)) {
            List<ApiCallEntity> calls = mongoTemplate.find(query, ApiCallEntity.class, callName);
            return calls.stream()
                    .filter(Objects::nonNull)
                    .filter(e -> !ApiMetricsDelayInfoUtil.checkByCode(e.getCode(), e.getHttpStatus()))
                    .map(ApiCallEntity::getAllPathId)
                    .distinct()
                    .toList();
        }
        return null;
    }

    protected Collection<String> errorCountOfSecondFiveRange(List<TimeRange> ranges, Consumer<Criteria> criteriaConsumer) {
        Criteria criteriaOfSec5 = Criteria.where("timeGranularity").is(TimeGranularity.SECOND_FIVE.getType());
        Optional.ofNullable(criteriaConsumer).ifPresent(c -> c.accept(criteriaOfSec5));
        Set<Long> times = ParticleSizeAnalyzer.asMinute(ranges);
        if (times.isEmpty()) {
            return null;
        }
        criteriaOfSec5.and("timeStart").in(times);
        criteriaOfSec5.and("errorCount").gt(0);
        List<ApiMetricsRaw> raws = service.find(Query.query(criteriaOfSec5));
        for (TimeRange point : ranges) {
            Set<String> apiIds = new HashSet<>();
            List<ApiMetricsRaw> rawsSub = ParticleSizeAnalyzer.secondFiveMetricsRaws(
                    raws,
                    e -> e.getTimeStart() >= point.getStart() && e.getTimeStart() < point.getEnd()
            );
            if (!rawsSub.isEmpty()) {
               rawsSub.stream().map(ApiMetricsRaw::getApiId).distinct().forEach(apiIds::add);
            }
            return apiIds;
        }
        return null;
    }

    protected Collection<String> errorCountOf(List<TimeRange> ranges, Consumer<Criteria> criteriaConsumer, TimeGranularity granularity) {
        Criteria criteriaOfHour = Criteria.where("timeGranularity").is(granularity.getType());
        List<Criteria> orHour = new ArrayList<>();
        Optional.ofNullable(criteriaConsumer).ifPresent(c -> c.accept(criteriaOfHour));
        for (TimeRange point : ranges) {
            orHour.add(Criteria.where("timeStart").gte(point.getStart()).lt(point.getEnd()));
        }
        criteriaOfHour.orOperator(orHour);
        criteriaOfHour.and("errorCount").gt(0);
        return count(criteriaOfHour);
    }

    protected Collection<String> count(Criteria criteria) {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(criteria),
                Aggregation.group("apiId"),
                Aggregation.project().and("_id").as("apiId")
        );
        AggregationResults<Map> results = apiMetricsRepository.aggregate(aggregation, Map.class);
        return results.getMappedResults().stream()
                .map(item -> {
                    Object apiId = item.get("apiId");
                    return apiId != null ? apiId.toString() : null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public Long errorCount(QueryBase param, Consumer<Criteria> criteriaConsumer, Criteria apiCallCriteria) {
        Set<String> errorApiIds = new HashSet<>();
        Map<TimeGranularity, List<TimeRange>> queryRange = param.getQueryRange();
        queryRange.forEach((timeGranularity, ranges) -> {
            if (ranges.isEmpty()) {
                return;
            }
            switch (timeGranularity) {
                case SECOND:
                    Optional.ofNullable(errorCountOfSecondRange(ranges, apiCallCriteria))
                            .ifPresent(errorApiIds::addAll);
                    break;
                case SECOND_FIVE:
                    Optional.ofNullable(errorCountOfSecondFiveRange(ranges, criteriaConsumer))
                            .ifPresent(errorApiIds::addAll);
                    break;
                default:
                    Optional.ofNullable(errorCountOf(ranges, criteriaConsumer, timeGranularity))
                            .ifPresent(errorApiIds::addAll);
            }
        });
        return (long) errorApiIds.size();
    }


    public long errorCountGetter(List<ApiMetricsRaw> rows, LongConsumer acceptReqCount) {
        long errorCount = 0L;
        for (final ApiMetricsRaw info : rows) {
            if (null == info) {
                continue;
            }
            acceptReqCount.accept(Optional.ofNullable(info.getReqCount()).orElse(0L));
            errorCount += Optional.ofNullable(info.getErrorCount()).orElse(0L);
        }
        return errorCount;
    }

    public <T extends ValueBase> void baseDataCalculate(T item, List<ApiMetricsRaw> apiMetricsRaws, LongConsumer valueSetter) {
        if (item instanceof DataValueBase result) {
            ApiMetricsDelayInfoUtil.Setter delaySetter = ApiMetricsDelayInfoUtil.Setter.of(valueSetter)
                    .avg(result::setResponseTimeAvg)
                    .max(result::setMaxDelay)
                    .min(result::setMinDelay)
                    .p95(result::setP95)
                    .p99(result::setP99);
            ApiMetricsDelayInfoUtil.calculate(apiMetricsRaws, ApiMetricsRaw::getDelay, delaySetter);
            ApiMetricsDelayInfoUtil.Setter dbCostSetter = ApiMetricsDelayInfoUtil.Setter.of(result::setDbCostTotal)
                    .avg(result::setDbCostAvg)
                    .max(result::setDbCostMax)
                    .min(result::setDbCostMin)
                    .p95(result::setDbCostP95)
                    .p99(result::setDbCostP99);
            ApiMetricsDelayInfoUtil.calculate(apiMetricsRaws, ApiMetricsRaw::getDbCost, dbCostSetter);
        }
    }

    protected Map<String, ModulesDto> publishApis() {
        List<ModulesDto> allActiveApi = (List<ModulesDto>) modulesService.findAllActiveApi(ModuleStatusEnum.ACTIVE);
        return allActiveApi.stream()
                .filter(Objects::nonNull)
                .filter(e -> Objects.nonNull(e.getId()))
                .collect(Collectors.toMap(e -> e.getId().toHexString(), e -> e, (e1, e2) -> e1));
    }
}
