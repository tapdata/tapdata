package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.constant.ModuleStatusEnum;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.utils.MongoUtils;
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
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsDelayInfoUtil;
import com.tapdata.tm.v2.api.monitor.utils.ApiPathUtil;
import com.tapdata.tm.v2.api.monitor.utils.ChartSortUtil;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
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

    public Page<TopApiInServer> topApiInServer(TopApiInServerParam param) {
        String serverId = param.getServerId();
        if (StringUtils.isBlank(serverId)) {
            throw new BizException(ApiMetricsRawQuery.SERVER_ID_EMPTY);
        }
        ParticleSizeAnalyzer.fixTime(param, false);
        List<ApiMetricsRaw> apiMetricsRaws = merge(
                param,
                c -> c.and(ApiMetricsRawQuery.PROCESS_ID).is(serverId).and("metricType").is(MetricTypes.API_SERVER.getType()),
                Criteria.where("api_gateway_uuid").is(serverId));
        Map<String, TopApiInServer> apiInfoMap = apiMetricsRaws.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getApiId()))
                .collect(
                        Collectors.groupingBy(
                                ApiMetricsRaw::getApiId,
                                Collectors.collectingAndThen(Collectors.toList(), rows -> {
                                    TopApiInServer item = TopApiInServer.create();
                                    item.setQueryFrom(param.getStartAt());
                                    item.setQueryEnd(param.getEndAt());
                                    item.setGranularity(param.getGranularity());
                                    long errorCount = errorCountGetter(rows, e -> item.setRequestCount(item.getRequestCount() + e));
                                    int total = item.getRequestCount().intValue();
                                    if (total > 0) {
                                        item.setErrorRate(ApiMetricsDelayInfoUtil.rate(errorCount, item.getRequestCount()));
                                        item.setErrorCount(errorCount);
                                        baseDataCalculate(item, rows, null);
                                    }
                                    return item;
                                })
                        ));
        List<ObjectId> apiIds = apiMetricsRaws.stream()
                .map(ApiMetricsRaw::getApiId)
                .distinct()
                .map(MongoUtils::toObjectId)
                .filter(Objects::nonNull)
                .toList();
        apiInfoMap.forEach((apiId, apiInfo) -> {
            apiInfo.setApiId(apiId);
            apiInfo.setApiName(apiId);
            apiInfo.setApiPath(apiId);
            apiInfo.setNotExistsApi(true);
        });
        if (CollectionUtils.isEmpty(apiIds)) {
            List<TopApiInServer> api = TopApiInServer.supplement(new ArrayList<>(), publishApis(), e -> TopApiInServer.create());
            return Page.page(api.stream().skip(param.getSkip()).limit(param.getLimit()).toList(), api.size());
        }
        Criteria criteriaOfApi = Criteria.where("_id").in(apiIds);
        Query queryOfApi = Query.query(criteriaOfApi);
        List<ModulesDto> apiDtoList = modulesService.findAll(queryOfApi);
        apiDtoList.forEach(apiDto -> {
            String apiId = apiDto.getId().toHexString();
            TopApiInServer item = apiInfoMap.computeIfAbsent(apiId, k -> TopApiInServer.create());
            String path = ApiPathUtil.apiPath(apiDto.getApiVersion(), apiDto.getBasePath(), apiDto.getPrefix());
            item.setApiId(apiId);
            item.setApiName(apiDto.getName());
            item.setApiPath(path);
            item.setNotExistsApi(false);
        });
        List<TopApiInServer> result = new ArrayList<>(apiInfoMap.values());
        TopApiInServer.supplement(result, publishApis(), e -> TopApiInServer.create());
        ChartSortUtil.sort(result, param.getSortInfo(), TopApiInServer.class);
        return Page.page(result.stream().skip(param.getSkip()).limit(param.getLimit()).toList(), result.size());
    }

    public Page<ApiItem> apiOverviewList(ApiListParam param) {
        ParticleSizeAnalyzer.fixTime(param, false);
        List<ApiMetricsRaw> apiMetricsRaws = merge(
                param,
                c -> c.and("metricType").is(MetricTypes.API.getType()),
                null);
        Map<String, ApiItem> apiInfoMap = apiMetricsRaws.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getApiId()))
                .collect(
                        Collectors.groupingBy(
                                ApiMetricsRaw::getApiId,
                                Collectors.collectingAndThen(Collectors.toList(), rows -> {
                                    ApiItem item = ApiItem.create();
                                    item.setQueryFrom(param.getStartAt());
                                    item.setQueryEnd(param.getEndAt());
                                    item.setGranularity(param.getGranularity());
                                    long errorCount = errorCountGetter(rows, e -> item.setRequestCount(item.getRequestCount() + e));
                                    int total = item.getRequestCount().intValue();
                                    if (total > 0) {
                                        item.setErrorRate(ApiMetricsDelayInfoUtil.rate(errorCount, item.getRequestCount()));
                                        item.setErrorCount(errorCount);
                                        baseDataCalculate(item, rows, null);
                                        long sumRps = rows.stream()
                                                .filter(Objects::nonNull)
                                                .map(ApiMetricsRaw::getBytes)
                                                .map(ApiMetricsDelayUtil::fixDelayAsMap)
                                                .map(ApiMetricsDelayUtil::sum)
                                                .map(ApiMetricsDelayUtil.Sum::getTotal)
                                                .mapToLong(Long::longValue)
                                                .sum();
                                        baseDataCalculate(item, rows, sumDelay -> item.setTotalRps(sumDelay > 0 ? 1000.0D * sumRps / sumDelay : 0D));
                                    }
                                    return item;
                                })
                        ));
        List<ObjectId> apiIds = apiMetricsRaws.stream()
                .map(ApiMetricsRaw::getApiId)
                .distinct()
                .map(MongoUtils::toObjectId)
                .filter(Objects::nonNull)
                .toList();
        apiInfoMap.forEach((apiId, apiInfo) -> {
            apiInfo.setApiId(apiId);
            apiInfo.setApiName(apiId);
            apiInfo.setApiPath(apiId);
            apiInfo.setNotExistsApi(true);
        });
        if (CollectionUtils.isEmpty(apiIds)) {
            List<ApiItem> api = TopApiInServer.supplement(new ArrayList<>(), publishApis(), e -> ApiItem.create());
            return Page.page(api.stream().skip(param.getSkip()).limit(param.getLimit()).toList(), api.size());
        }
        Criteria criteriaOfApi = Criteria.where("_id").in(apiIds);
        Query queryOfApi = Query.query(criteriaOfApi);
        List<ModulesDto> apiDtoList = modulesService.findAll(queryOfApi);
        apiDtoList.forEach(apiDto -> {
            String apiId = apiDto.getId().toHexString();
            TopApiInServer item = apiInfoMap.computeIfAbsent(apiId, k -> ApiItem.create());
            String path = ApiPathUtil.apiPath(apiDto.getApiVersion(), apiDto.getBasePath(), apiDto.getPrefix());
            item.setApiId(apiId);
            item.setApiName(apiDto.getName());
            item.setApiPath(path);
            item.setNotExistsApi(false);
        });
        List<ApiItem> result = new ArrayList<>(apiInfoMap.values());
        TopApiInServer.supplement(result, publishApis(), e -> ApiItem.create());
        ChartSortUtil.sort(result, param.getSortInfo(), ApiItem.class);
        return Page.page(result.stream().skip(param.getSkip()).limit(param.getLimit()).toList(), result.size());
    }

    public List<ApiMetricsRaw> merge(QueryBase param, Consumer<Criteria> criteriaConsumer, Criteria apiCallCriteria) {
        List<ApiMetricsRaw> metricsRawList = new ArrayList<>();
        Map<TimeGranularity, List<ParticleSizeAnalyzer.TimeRange>> queryRange = param.getQueryRange();
        queryRange.forEach((timeGranularity, ranges) -> {
            if (ranges.isEmpty()) {
                return;
            }
            switch (timeGranularity) {
                case SECOND:
                    List<Criteria> andCriteria = new ArrayList<>();
                    andCriteria.add(Criteria.where("delete").ne(true)
                            .and("req_path").nin(MetricInstanceFactory.IGNORE_PATH));
                    Optional.ofNullable(apiCallCriteria).ifPresent(andCriteria::add);
                    List<Criteria> orSec = new ArrayList<>();
                    for (ParticleSizeAnalyzer.TimeRange point : ranges) {
                        orSec.add(Criteria.where("reqTime").gte(point.getStart() * 1000L).lt(point.getEnd() * 1000L));
                    }
                    andCriteria.add(new Criteria().orOperator(orSec));
                    Query query = Query.query(new Criteria().andOperator(andCriteria));
                    query.fields().include("api_gateway_uuid", "allPathId", "reqTime", "code", "httpStatus", "req_bytes", "latency", "_id");
                    String callName = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
                    if (StringUtils.isNotBlank(callName)) {
                        List<ApiCallEntity> calls = mongoTemplate.find(query, ApiCallEntity.class, callName);
                        ParticleSizeAnalyzer.parseToMetric(calls, metricsRawList::add);
                    }
                    break;
                case SECOND_FIVE:
                    Criteria criteriaOfSec5 = Criteria.where("timeGranularity").is(1);
                    Optional.ofNullable(criteriaConsumer).ifPresent(c -> c.accept(criteriaOfSec5));
                    //List<Criteria> orSec5 = new ArrayList<>();
                    List<Long> times = new ArrayList<>();
                    for (ParticleSizeAnalyzer.TimeRange point : ranges) {
                        times.add(point.getStart() / 60L * 60L);
                        //orSec5.add(Criteria.where("timeStart").gte(point.getStart()).lt(point.getEnd()));
                    }
                    //criteriaOfSec5.orOperator(orSec5);
                    criteriaOfSec5.and("timeStart").in(times);
                    Query querySec5 = Query.query(criteriaOfSec5);
                    QueryBase of5Sec = new QueryBase();
                    of5Sec.setGranularity(0);
                    querySec5.fields().include("apiId", "processId", "timeGranularity", "timeStart", "subMetrics");
                    List<ApiMetricsRaw> metricsRawListSec5 = service.find(querySec5);
                    for (ParticleSizeAnalyzer.TimeRange point : ranges) {
                        of5Sec.setStartAt(point.getStart());
                        of5Sec.setEndAt(point.getEnd());
                        of5Sec.setQStart(point.getStart());
                        List<ApiMetricsRaw> raws = ParticleSizeAnalyzer.apiMetricsRaws(metricsRawListSec5, of5Sec);
                        metricsRawList.addAll(raws);
                    }
                    break;
                case MINUTE:
                    Criteria criteriaMin = Criteria.where("timeGranularity").is(1);
                    Optional.ofNullable(criteriaConsumer).ifPresent(c -> c.accept(criteriaMin));
                    List<Criteria> or = new ArrayList<>();
                    for (ParticleSizeAnalyzer.TimeRange point : ranges) {
                        or.add(Criteria.where("timeStart").gte(point.getStart()).lt(point.getEnd()));
                    }
                    criteriaMin.orOperator(or);
                    Query queryMin = Query.query(criteriaMin);
                    queryMin.fields().include("apiId", "processId", "timeGranularity", "timeStart", "reqCount", "errorCount", "bytes", "delay", "dbCost");
                    List<ApiMetricsRaw> metricsRawListMin = service.find(queryMin);
                    metricsRawList.addAll(metricsRawListMin);
                    break;
                case HOUR:
                    Criteria criteriaOfHour = Criteria.where("timeGranularity").is(2);
                    Optional.ofNullable(criteriaConsumer).ifPresent(c -> c.accept(criteriaOfHour));
                    List<Criteria> orHour = new ArrayList<>();
                    for (ParticleSizeAnalyzer.TimeRange point : ranges) {
                        orHour.add(Criteria.where("timeStart").gte(point.getStart()).lt(point.getEnd()));
                    }
                    criteriaOfHour.orOperator(orHour);
                    Query queryHour = Query.query(criteriaOfHour);
                    queryHour.fields().include("apiId", "processId", "timeGranularity", "timeStart", "reqCount", "errorCount", "bytes", "delay", "dbCost");
                    List<ApiMetricsRaw> metricsRawListHour = service.find(queryHour);
                    metricsRawList.addAll(metricsRawListHour);
                    break;
                default:
                    Criteria criteriaOfDay = Criteria.where("timeGranularity").is(3);
                    Optional.ofNullable(criteriaConsumer).ifPresent(c -> c.accept(criteriaOfDay));
                    List<Criteria> orDay = new ArrayList<>();
                    for (ParticleSizeAnalyzer.TimeRange point : ranges) {
                        orDay.add(Criteria.where("timeStart").gte(point.getStart()).lt(point.getEnd()));
                    }
                    criteriaOfDay.orOperator(orDay);
                    Query queryDay = Query.query(criteriaOfDay);
                    queryDay.fields().include("apiId", "processId", "timeGranularity", "timeStart", "reqCount", "errorCount", "bytes", "delay", "dbCost");
                    List<ApiMetricsRaw> metricsRawListDay = service.find(queryDay);
                    metricsRawList.addAll(metricsRawListDay);
            }
        });
        return metricsRawList;
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
