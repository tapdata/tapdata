package com.tapdata.tm.dataflowinsight.service;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.entity.DataFlow;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.dataflowinsight.dto.DataFlowInsightDto;
import com.tapdata.tm.dataflowinsight.dto.DataFlowInsightStatisticsDto;
import com.tapdata.tm.dataflowinsight.dto.RuntimeMonitorReq;
import com.tapdata.tm.dataflowinsight.dto.RuntimeMonitorResp;
import com.tapdata.tm.dataflowinsight.entity.DataFlowInsightEntity;
import com.tapdata.tm.dataflowinsight.repository.DataFlowInsightRepository;
import com.tapdata.tm.utils.MapUtils;
import static com.tapdata.tm.utils.MongoUtils.toObjectId;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AddFieldsOperation;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.LookupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2021/09/13
 * @Description:
 */
@Service
@Slf4j
public class DataFlowInsightService extends BaseService<DataFlowInsightDto, DataFlowInsightEntity, ObjectId, DataFlowInsightRepository> {


    public static final Map<String, List<String>> granularityMap = Collections.unmodifiableMap(new HashMap<String, List<String>>() {{
        put("minute", Arrays.asList("flow_minute", "stage_minute", "minute"));
        put("hour", Arrays.asList("flow_hour", "stage_hour", "hour"));
        put("day", Arrays.asList("flow_day", "stage_day", "day"));
        put("second", Arrays.asList("flow", "second", "flow_second" ,"stage_second"));
    }});


    private static final DateTimeFormatter secondSdf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter minuteSdf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:00");
    private static final DateTimeFormatter hourSdf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00:00");
    private static final DateTimeFormatter daySdf = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00");

    @Autowired
    private DataFlowService dataFlowService;

    @Autowired
    private MongoTemplate mongoTemplate;

    public DataFlowInsightService(@NonNull DataFlowInsightRepository repository) {
        super(repository, DataFlowInsightDto.class, DataFlowInsightEntity.class);
    }

    protected void beforeSave(DataFlowInsightDto dataFlowInsight, UserDetail user) {

    }

    public RuntimeMonitorResp runtimeMonitor(RuntimeMonitorReq runtimeMonitorReq, UserDetail userDetail){

        Map<String, Map<String, List<DataFlowInsightDto>>> resultMap = new HashMap<>();
	    DataFlowDto dataFlowDto = dataFlowService.findById(toObjectId(runtimeMonitorReq.getDataFlowId()), userDetail);
	    if (dataFlowDto == null){
	    	throw new BizException("DataFlow.Not.Found");
	    }
	    String granularity = granularityMap.entrySet().stream()
                .filter(entry -> entry.getValue().contains(runtimeMonitorReq.getGranularity()))
                .findFirst()
                .map(Map.Entry::getKey)
                .orElse(runtimeMonitorReq.getGranularity());
        List<DataFlowInsightDto> dataFlowInsightDtos = findDataFlowInsight(granularity, runtimeMonitorReq.getDataFlowId(), userDetail);
        resultMap.put(granularity, new HashMap<String, List<DataFlowInsightDto>>(){{ put(runtimeMonitorReq.getDataFlowId(), dataFlowInsightDtos);}});
        Object statsData;
        switch (runtimeMonitorReq.getStatsType()){
            case "throughput":
                statsData = getThroughputResults(resultMap, runtimeMonitorReq.getGranularity(), runtimeMonitorReq.getDataFlowId(), runtimeMonitorReq.getStageId());
                break;
            case "trans_time":
                statsData = getTransTimeResults(resultMap, runtimeMonitorReq.getGranularity(), runtimeMonitorReq.getDataFlowId(), runtimeMonitorReq.getStageId());
                break;
            case "repl_lag":
                statsData = getReplLagResults(resultMap, runtimeMonitorReq.getGranularity(), runtimeMonitorReq.getDataFlowId(), runtimeMonitorReq.getStageId());
                break;
            case "data_overview":
                statsData = getDataOverviewResult(resultMap, runtimeMonitorReq.getGranularity(), runtimeMonitorReq.getDataFlowId(), runtimeMonitorReq.getStageId());
                break;
            default:
                throw new BizException("IllegalArgument", "statsType");
        }
        RuntimeMonitorResp runtimeMonitorResp = new RuntimeMonitorResp();
        runtimeMonitorResp.setStatsType(runtimeMonitorReq.getStatsType());
        runtimeMonitorResp.setCreateTime(new Date());
        runtimeMonitorResp.setDataFlowId(runtimeMonitorReq.getDataFlowId());
        runtimeMonitorResp.setGranularity(runtimeMonitorReq.getGranularity());
        runtimeMonitorResp.setStatsData(statsData);
        return runtimeMonitorResp;
    }

    public List<Map<String, Object>> getThroughputResults(Map<String, Map<String, List<DataFlowInsightDto>>> resultMap,
                                                           String throughput, String idsStr, String stageId){

        List<Map<String, Object>> throughputResults = new ArrayList<>();
        List<DataFlowInsightDto> insightDtosWithThroughput = getCacheDataBygranularity(resultMap, throughput).get(idsStr);

        AtomicLong currentTimeMillis = new AtomicLong(System.currentTimeMillis());
        if (CollectionUtils.isNotEmpty(insightDtosWithThroughput)) {
            String cacheKey = getCacheKeyBygranularity(throughput);
            for (int i = 0; i < insightDtosWithThroughput.size(); i++) {
                DataFlowInsightDto insightDtoWithThroughput = insightDtosWithThroughput.get(i);
                double deltaTime = 1; //(curItem.last_updated.getTime() - curItem.id.getTimestamp().getTime()) / 1000;
                DataFlowInsightDto preInsightDtoWithThroughput = null;
                Date createTime = null;
                if (i + 1 < insightDtosWithThroughput.size()){
                    preInsightDtoWithThroughput = insightDtosWithThroughput.get(i + 1);
                    long diffTime = (insightDtoWithThroughput.getCreateAt().getTime() - preInsightDtoWithThroughput.getCreateAt().getTime()) / 1000;
                    deltaTime = diffTime > 0 ? diffTime : deltaTime;
                    createTime = preInsightDtoWithThroughput.getCreateAt();
                }else if (insightDtosWithThroughput.size() < 20){
                    switch (cacheKey) {
                        case "second":
                            deltaTime = 1;
                            break;
                        case "minute":
                            deltaTime = 60;
                            break;
                        case "hour":
                            deltaTime = 60 * 60;
                            break;
                        case "day":
                            deltaTime = 24 * 60 * 60;
                            break;
                    }
                    createTime = insightDtoWithThroughput.getCreateAt();
                }

                if (i < 20){
                    Map<String, Object> statsData = new HashMap<>();
                    if (createTime != null){
                        switch (cacheKey) {
                            case "second":
                                statsData.put("t", secondSdf.format(date2LocalDateTime(createTime)));
                                break;
                            case "minute":
                                statsData.put("t", minuteSdf.format(date2LocalDateTime(createTime)));
                                break;
                            case "hour":
                                statsData.put("t", hourSdf.format(date2LocalDateTime(createTime)));
                                break;
                            case "day":
                                statsData.put("t", daySdf.format(date2LocalDateTime(createTime)));
                                break;

                        }
                    }else {
                        statsData.put("t", getTimeByGranularity(throughput, currentTimeMillis));
                    }
                    Map<String, Double> calculation = calculation(insightDtoWithThroughput, preInsightDtoWithThroughput, stageId);
                    if (MapUtils.isNotEmpty(calculation)){
                        statsData.put("inputCount", Math.ceil(calculation.get("inputCount")/deltaTime));
                        statsData.put("outputCount", Math.ceil(calculation.get("outputCount")/deltaTime));
                        statsData.put("inputSize", Math.ceil(calculation.get("inputSize")/1024/deltaTime));
                        statsData.put("outputSize", Math.ceil(calculation.get("outputSize")/1024/deltaTime));
                        throughputResults.add(statsData);
                    }
                }

            }
        }

        if (throughputResults.size() < 20){
            int size = throughputResults.size();
            for (int j = 0; j < 20 - size; j++) {
                Map<String, Object> map = new HashMap<>();
                map.put("t", getTimeByGranularity(throughput, currentTimeMillis));
                map.put("inputSize", 0);
                map.put("outputSize", 0);
                map.put("inputCount", 0);
                map.put("outputCount", 0);
                throughputResults.add(map);
            }
        }
        sort(throughputResults);

        return throughputResults;
    }
    public List<Map<String, Object>> getTransTimeResults(Map<String, Map<String, List<DataFlowInsightDto>>> resultMap,
                                                          String transTime, String idsStr, String stageId){

        List<Map<String, Object>> transTimeResults = new ArrayList<>();
        List<DataFlowInsightDto> insightDtosWithTransTime = getCacheDataBygranularity(resultMap, transTime).get(idsStr);

        AtomicLong currentTimeMillis = new AtomicLong(System.currentTimeMillis());
        if (CollectionUtils.isNotEmpty(insightDtosWithTransTime)) {
            String cacheKey = getCacheKeyBygranularity(transTime);
            for (int i = 0; i < insightDtosWithTransTime.size(); i++) {
                DataFlowInsightDto insightDtoWithTransTime = insightDtosWithTransTime.get(i);
                Map<String, Object> statsData = getStatsData(insightDtoWithTransTime, stageId);
                String rows = MapUtils.getAsStringByPath(statsData, "input/rows");
                DataFlowInsightDto preInsightDtoWithTransTime = null;
                double transmissionTime = 0d;
                Date createTime = null;
                if (i + 1 < insightDtosWithTransTime.size()){
                    preInsightDtoWithTransTime = insightDtosWithTransTime.get(i + 1);
                    Map<String, Object> preStatsData = getStatsData(preInsightDtoWithTransTime, stageId);
                    String preRows = MapUtils.getAsStringByPath(preStatsData, "input/rows");
                    if (StringUtils.isNotBlank(rows) && StringUtils.isNotBlank(preRows)){
                        double deltaRows = Double.parseDouble(rows) - Double.parseDouble(preRows);
                        Long transmissionTimeByMap = MapUtils.getAsLong(statsData, "transmissionTime");
                        Long preTransmissionTime = MapUtils.getAsLong(preStatsData, "transmissionTime");
                        double time = deltaRows > 0 ? (transmissionTimeByMap - preTransmissionTime) / deltaRows : 0;
                        transmissionTime = Double.parseDouble(new DecimalFormat("#0.00").format(time));
                        createTime = preInsightDtoWithTransTime.getCreateAt();
                    }
                }else if (insightDtosWithTransTime.size() < 20){
                    if (StringUtils.isNotBlank(rows)){
                        double deltaRows = Double.parseDouble(rows);
                        Long transmissionTimeByMap = MapUtils.getAsLong(statsData, "transmissionTime");
                        double time = deltaRows > 0 ? transmissionTimeByMap / deltaRows : 0;
                        transmissionTime = Double.parseDouble(new DecimalFormat("#0.00").format(time));
                        createTime = insightDtoWithTransTime.getCreateAt();
                    }
                }

                if (i < 20){
                    Map<String, Object> statsDataMap = new HashMap<>();
                    statsDataMap.put("d", transmissionTime);
                    if (createTime != null){
                        switch (cacheKey) {
                            case "second":
                                statsDataMap.put("t", secondSdf.format(date2LocalDateTime(createTime)));
                                break;
                            case "minute":
                                statsDataMap.put("t", minuteSdf.format(date2LocalDateTime(createTime)));
                                break;
                            case "hour":
                                statsDataMap.put("t", hourSdf.format(date2LocalDateTime(createTime)));
                                break;
                            case "day":
                                statsDataMap.put("t", daySdf.format(date2LocalDateTime(createTime)));
                                break;

                        }

                    }else {
                        statsDataMap.put("t", getTimeByGranularity(transTime, currentTimeMillis));
                    }
                    transTimeResults.add(statsDataMap);
                }

            }
        }

        if (transTimeResults.size() < 20){
            int size = transTimeResults.size();
            for (int j = 0; j < 20 - size; j++) {
                Map<String, Object> map = new HashMap<>();
                map.put("t", getTimeByGranularity(transTime, currentTimeMillis));
                map.put("d", 0);
                transTimeResults.add(map);
            }
        }
        sort(transTimeResults);

        return transTimeResults;
    }

    public List<Map<String, Object>> getReplLagResults(Map<String, Map<String, List<DataFlowInsightDto>>> resultMap,
                                                        String replLag, String idsStr, String stageId){

        List<Map<String, Object>> replLagResults = new ArrayList<>();
        List<DataFlowInsightDto> insightDtosWithReplLag = getCacheDataBygranularity(resultMap, replLag).get(idsStr);

        AtomicLong currentTimeMillis = new AtomicLong(System.currentTimeMillis());
        if (CollectionUtils.isNotEmpty(insightDtosWithReplLag)) {
            String cacheKey = getCacheKeyBygranularity(replLag);
            for (int i = 0; i < insightDtosWithReplLag.size(); i++) {
                DataFlowInsightDto insightDtoWithReplLag = insightDtosWithReplLag.get(i);
                Map<String, Object> statsData = getStatsData(insightDtoWithReplLag, stageId);

                if (i < 20){
                    Map<String, Object> statsDataMap = new HashMap<>();
                    statsDataMap.put("d", statsData.get("replicationLag"));
                    Date createTime = insightDtoWithReplLag.getCreateAt();
                    if (createTime != null){
                        switch (cacheKey) {
                            case "second":
                                statsDataMap.put("t", secondSdf.format(date2LocalDateTime(createTime)));
                                break;
                            case "minute":
                                statsDataMap.put("t", minuteSdf.format(date2LocalDateTime(createTime)));
                                break;
                            case "hour":
                                statsDataMap.put("t", hourSdf.format(date2LocalDateTime(createTime)));
                                break;
                            case "day":
                                statsDataMap.put("t", daySdf.format(date2LocalDateTime(createTime)));
                                break;

                        }
                    }else {
                        statsDataMap.put("t", getTimeByGranularity(replLag, currentTimeMillis));
                    }
                    replLagResults.add(statsDataMap);
                }

            }
        }

        if (replLagResults.size() < 20){
            int size = replLagResults.size();
            for (int j = 0; j < 20 - size; j++) {
                Map<String, Object> map = new HashMap<>();
                map.put("t", getTimeByGranularity(replLag, currentTimeMillis));
                map.put("d", 0);
                replLagResults.add(map);
            }
        }
        sort(replLagResults);

        return replLagResults;
    }

    public Map<String, Object> getDataOverviewResult(Map<String, Map<String, List<DataFlowInsightDto>>> resultMap,
                                                      String dataOverview, String idsStr, String stageId){

        Map<String, Object> dataOverviewResult = new HashMap<>();
        List<DataFlowInsightDto> insightDtosWithDataOverview = getCacheDataBygranularity(resultMap, dataOverview).get(idsStr);

        if (CollectionUtils.isEmpty(insightDtosWithDataOverview)) {
            return dataOverviewResult;
        }

        dataOverviewResult.put("t", secondSdf.format(LocalDateTime.now()));
        Map<String, Object> statsData = getStatsData(insightDtosWithDataOverview.get(0), stageId);
        dataOverviewResult.put("inputCount", MapUtils.getValueByPatchPath(statsData, "input/rows"));
        dataOverviewResult.put("outputCount", MapUtils.getValueByPatchPath(statsData, "output/rows"));

        Object insert = MapUtils.getValueByPatchPath(statsData, "insert/rows");
        dataOverviewResult.put("insertCount", insert != null ? insert : 0);
        Object update = MapUtils.getValueByPatchPath(statsData, "update/rows");
        dataOverviewResult.put("updateCount", update != null ? update : 0);
        Object delete = MapUtils.getValueByPatchPath(statsData, "delete/rows");
        dataOverviewResult.put("deleteCount", delete != null ? delete : 0);

        Object inputSize = MapUtils.getValueByPatchPath(statsData, "input/dataSize");
        dataOverviewResult.put("inputSize", inputSize != null ? Math.ceil(Double.parseDouble(inputSize.toString())/1024) : 0);
        Object outputSize = MapUtils.getValueByPatchPath(statsData, "output/dataSize");
        dataOverviewResult.put("inputSize", outputSize != null ? Math.ceil(Double.parseDouble(outputSize.toString())/1024) : 0);

        Object insertSize = MapUtils.getValueByPatchPath(statsData, "insert/dataSize");
        dataOverviewResult.put("insertSize", insertSize != null ? Math.ceil(Double.parseDouble(insertSize.toString())/1024) : 0);
        Object updateSize = MapUtils.getValueByPatchPath(statsData, "update/dataSize");
        dataOverviewResult.put("updateSize", updateSize != null ? Math.ceil(Double.parseDouble(updateSize.toString())/1024) : 0);
        Object deleteSize = MapUtils.getValueByPatchPath(statsData, "delete/dataSize");
        dataOverviewResult.put("deleteSize", deleteSize != null ? Math.ceil(Double.parseDouble(deleteSize.toString())/1024) : 0);

        return dataOverviewResult;
    }


    private void sort(List<Map<String, Object>> list){
        list.sort((m1, m2) ->{
            try {
                return LocalDateTime.parse(m1.get("t").toString(), secondSdf).compareTo(LocalDateTime.parse(m2.get("t").toString(), secondSdf));
            } catch (Exception e) {
                return 0;
            }
        });
    }

    private Map<String, List<DataFlowInsightDto>> getCacheDataBygranularity(Map<String, Map<String, List<DataFlowInsightDto>>> resultMap, String value){
        if (MapUtils.isEmpty(resultMap) || StringUtils.isBlank(value)){
            return Collections.emptyMap();
        }

        String key = getCacheKeyBygranularity(value);
        if (StringUtils.isBlank(key)){
            return Collections.emptyMap();
        }

        return resultMap.get(key);
    }

    private String getCacheKeyBygranularity(String value){

        return granularityMap.entrySet().stream().filter(entry -> entry.getValue().contains(value)).findFirst().map(Map.Entry::getKey).orElse(null);
    }

    private String getTimeByGranularity(String granularity, AtomicLong currentTimeMillis){
        String t= "";
        String cacheKey = getCacheKeyBygranularity(granularity);
        if (StringUtils.isBlank(cacheKey)){
            return t;
        }
        switch (cacheKey) {
            case "second":
                t = secondSdf.format(date2LocalDateTime(new Date(currentTimeMillis.addAndGet( - 1000 * 5))));
                break;
            case "minute":
                t = minuteSdf.format(date2LocalDateTime(new Date(currentTimeMillis.addAndGet( - 1000 * 60))));
                break;
            case "hour":
                t = hourSdf.format(date2LocalDateTime(new Date(currentTimeMillis.addAndGet( - 1000 * 60 * 60))));
                break;
            case "day":
                t = daySdf.format(date2LocalDateTime(new Date(currentTimeMillis.addAndGet( - 1000 * 60 * 60 * 24))));
                break;

        }

        return t;
    }


    private Map<String, Object> getStageMetric(Map<String, Object> statsData, String stageId) {
        if (StringUtils.isNotBlank(stageId)){
            List<Map<String, Object>> stagesMetrics = MapUtils.getAsList(statsData, "stagesMetrics");
            if (CollectionUtils.isNotEmpty(stagesMetrics)){
                for (Map<String, Object> stageMetric : stagesMetrics) {
                    if (stageMetric != null
                            && stageId.equals(MapUtils.getAsString(stageMetric, "stageId"))){
                        return stageMetric;
                    }
                }
            }
        }

        return null;
    }

    private double getDiffMetrics(Map<String, Object> statsData, Map<String, Object> prestatsData, String index){
        double result = 0L;
        String value = MapUtils.getAsStringByPath(statsData, index);
        if (StringUtils.isNotBlank(value)){
            result = Double.parseDouble(value);
            String preValue = MapUtils.getAsStringByPath(prestatsData, index);
            if (StringUtils.isNotBlank(preValue)){
                result -= Double.parseDouble(preValue);
            }
        }

        return result > 0 ? result : 0;
    }

    private Map<String, Double> calculation(DataFlowInsightDto dataFlowInsightDto, DataFlowInsightDto preDataFlowInsightDto, String stageId){
        Map<String, Double> result = new HashMap<>();
        Map<String, Object> statsData = dataFlowInsightDto.getStatsData();
        Map<String, Object> prestatsData = preDataFlowInsightDto != null ? preDataFlowInsightDto.getStatsData() : null;
        if (StringUtils.isNotBlank(stageId)){
            Map<String, Object> stageMetric = getStageMetric(statsData, stageId);
            if (stageMetric != null){
                statsData = stageMetric;
            }
            Map<String, Object> preStageMetric = getStageMetric(prestatsData, stageId);
            if (preStageMetric != null){
                prestatsData = preStageMetric;
            }
        }
        result.put("inputCount", getDiffMetrics(statsData, prestatsData, "input/rows"));
        result.put("outputCount", getDiffMetrics(statsData, prestatsData, "output/rows"));
        result.put("inputSize", getDiffMetrics(statsData, prestatsData, "input/dataSize"));
        result.put("outputSize", getDiffMetrics(statsData, prestatsData, "output/dataSize"));

        return result;
    }

    private Map<String, Object> getStatsData(DataFlowInsightDto dataFlowInsightDto, String stageId){
        Map<String, Object> statsData = dataFlowInsightDto.getStatsData();
        Map<String, Object> stageMetric = getStageMetric(statsData, stageId);
        if (stageMetric != null){
            statsData = stageMetric;
        }

        return statsData == null ? Collections.emptyMap() : statsData;
    }

    public List<DataFlowInsightDto> findDataFlowInsight(String granularity, String ids){

        return findDataFlowInsight(granularity, ids, null);
    }

    private List<DataFlowInsightDto> findDataFlowInsight(String granularity, String ids, UserDetail userDetail){
        if (StringUtils.isBlank(ids) || ids.startsWith(",")) {
            return Collections.emptyList();
        }
        String[] split = ids.split(",");
        if (split.length == 0){
            return Collections.emptyList();
        }
        Criteria criteria = Criteria.where("statsType").is("runtime_stats")
                .and("granularity").is(granularity)
                .and("dataFlowId").is(split[0]);
        if (split.length > 1){
            criteria.and("statsData.stagesMetrics.stageId").is(split[1]);
        }
        Query query = Query.query(criteria)
                .with(Sort.by(Sort.Direction.DESC, "_id"))
                .limit(21);
//        return userDetail != null ? convertToDto(findAll(query, userDetail), DataFlowInsightDto.class) : findAll(query);
        return findAll(query);
    }

    /**
     * 当日：按照小时的粒度来展示，当日每小时的输入数据量
     * 最近一周：按照天的粒度来展示，最近一周（当前日期往后推7天）每天的输入数据量
     * 最近一月：按照天的粒度来展示，最近一个月（当前日期往后推30天）每天的输入数据量
     *
     * @param type 当日:day, 最近一周:week, 最近一月:month
     **/
    public DataFlowInsightStatisticsDto statistics(String type, UserDetail userDetail) {

        DataFlowInsightStatisticsDto dataFlowInsightStatisticsDto = new DataFlowInsightStatisticsDto();
        dataFlowInsightStatisticsDto.setGranularity(type);
        String granularity;
        DateTimeFormatter formatter, dateTimeFormatter;
        LocalDate localDate = LocalDate.now();
        int size;
        ChronoUnit chronoUnit;
        switch (type) {
            case "day":
                granularity = "hour";
                localDate = localDate.minusDays(1);
                formatter = DateTimeFormatter.ofPattern("yyyyMMddHH0000");
                dateTimeFormatter = DateTimeFormatter.ofPattern("HH:00");
                size = 24;
                chronoUnit = ChronoUnit.HOURS;
                break;
            case "week":
                granularity = "day";
                localDate = localDate.minusDays(7);
                formatter = DateTimeFormatter.ofPattern("yyyyMMdd000000");
                dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                size = 7;
                chronoUnit = ChronoUnit.DAYS;
                break;
            case "month":
                granularity = "day";
                localDate = localDate.minusDays(30);
                formatter = DateTimeFormatter.ofPattern("yyyyMMdd000000");
                dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                size = 30;
                chronoUnit = ChronoUnit.DAYS;
                break;
            default:
                return dataFlowInsightStatisticsDto;
        }

        Query query = new Query();
        query.fields().include("stats.input.rows");
        List<DataFlow> dataFlows = dataFlowService.findAll(query, userDetail);
        List<String> dataFlowIdList = new ArrayList<>();
        AtomicLong totalInputDataCount = new AtomicLong();
        // dataFlow的stats中input.rows为0的不需要统计
        dataFlows.stream().filter(dataFlow -> dataFlow.getStats() != null).forEach(dataFlow -> {
            String rows = MapUtils.getAsStringByPath(dataFlow.getStats(), "/input/rows");
            if (StringUtils.isBlank(rows) || Long.parseLong(rows) == 0) {
                return;
            }
            totalInputDataCount.addAndGet(Long.parseLong(rows));
            dataFlowIdList.add(dataFlow.getId().toHexString());
        });
        Map<String, Long> dataStatistics = new HashMap<>();
        double ceil = Math.ceil(dataFlowIdList.size() / 50.0);
        for (int i = 0; i < ceil; i++) {
            setInputDataStatistics(dataFlowIdList.subList(i * 50, Math.min((i + 1) * 50, dataFlowIdList.size())), localDate, granularity, size, dataStatistics);
        }

        LocalDateTime localDateTime = LocalDateTime.of(LocalDate.now(), LocalTime.MAX);
        List<DataFlowInsightStatisticsDto.DataStatisticInfo> dataStatisticInfos = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            dataStatisticInfos.add(new DataFlowInsightStatisticsDto.DataStatisticInfo(localDateTime.format(dateTimeFormatter), dataStatistics.getOrDefault(localDateTime.format(formatter), 0L)));
            localDateTime = localDateTime.minus(1, chronoUnit);
        }
        dataStatisticInfos.sort(Comparator.comparing(DataFlowInsightStatisticsDto.DataStatisticInfo::getTime));
        dataFlowInsightStatisticsDto.setInputDataStatistics(dataStatisticInfos);
        dataFlowInsightStatisticsDto.setTotalInputDataCount(totalInputDataCount.get());
        return dataFlowInsightStatisticsDto;
    }

    private void setInputDataStatistics(List<String> dataFlowIdList, LocalDate localDate, String granularity, int size, Map<String, Long> dataStatistics){
        Query query = Query.query(Criteria.where("granularity").is(granularity).and("dataFlowId").in(dataFlowIdList))
                .with(Sort.by(Sort.Direction.DESC, "createTime"))
                /*.limit(size)*/;
        query.fields().include("dataFlowId", "statsTime", "statsData.input.rows", "createAt");
        List<DataFlowInsightDto> dataFlowInsightDtos = findAll(query);
        Map<String, Map<String, DataFlowInsightDto>> map = new HashMap<>();
        // 取出dataflow下的输入统计
        for (DataFlowInsightDto dataFlowInsightDto : dataFlowInsightDtos) {
            if (!map.containsKey(dataFlowInsightDto.getDataFlowId())) {
                map.put(dataFlowInsightDto.getDataFlowId(), new HashMap<>());
            }
            if (!map.get(dataFlowInsightDto.getDataFlowId()).containsKey(dataFlowInsightDto.getStatsTime())) {
                map.get(dataFlowInsightDto.getDataFlowId()).put(dataFlowInsightDto.getStatsTime(), dataFlowInsightDto);
            }
        }

        for (Map<String, DataFlowInsightDto> value : map.values()) {
            List<DataFlowInsightDto> insightDtos = new ArrayList<>(value.values());
            // 输入统计根据时间排序
            insightDtos.sort((i1, i2) -> (int) (i2.getCreateAt().getTime() - i1.getCreateAt().getTime()));
            if (CollectionUtils.isNotEmpty(insightDtos)){
                Map<String, Long> rowsMap = new HashMap<>();
                // 比指定时间更早的第一个时间点，用来比较指定时间内最早点的差值，比如00:00:00之前的一条数据，相减获得0点的数据输入值
                AtomicInteger recentIndex = new AtomicInteger(-1);
                IntStream.range(0, insightDtos.size() - 1).forEachOrdered(i -> {
                    LocalDate date2LocalDate = date2LocalDate(insightDtos.get(i).getCreateAt());
                    // 比较指定时间之后的输入数据差
                    if (date2LocalDate != null && date2LocalDate.isAfter(localDate)) {
                        String now = MapUtils.getAsStringByPath(insightDtos.get(i).getStatsData(), "/input/rows");
                        String prev = MapUtils.getAsStringByPath(insightDtos.get(i + 1).getStatsData(), "/input/rows");
                        if (StringUtils.isNotBlank(now) && StringUtils.isNotBlank(prev)) {
                            long l = Long.parseLong(now) - Long.parseLong(prev);
                            String key = insightDtos.get(i).getStatsTime();
                            if (!rowsMap.containsKey(key)) {
                                rowsMap.put(key, l > 0 ? l : 0);
                            }
                        }
                    }else if (recentIndex.get() < 0){
                        recentIndex.set(i);
                    }
                });
                if (rowsMap.size() < size) {
                    int index = recentIndex.get() > 0 ? recentIndex.get() - 1 : insightDtos.size() - 1;
                    String key = insightDtos.get(index).getStatsTime();
                    LocalDate date2LocalDate = date2LocalDate(insightDtos.get(index).getCreateAt());
                    if (date2LocalDate != null && date2LocalDate.isAfter(localDate) && !rowsMap.containsKey(key)) {
                        String rows = MapUtils.getAsStringByPath(insightDtos.get(index).getStatsData(), "/input/rows");
                        rowsMap.put(key, StringUtils.isNotBlank(rows) ? Long.parseLong(rows) : 0);
                    }
                }

                rowsMap.forEach((key, v) -> {
                    if (!dataStatistics.containsKey(key)) {
                        dataStatistics.put(key, v > 0 ? v : 0L);
                    } else {
                        dataStatistics.put(key, dataStatistics.get(key) + (v > 0 ? v : 0));
                    }
                });
            }
        }
    }

    private LocalDate date2LocalDate(Date date) {
        if(date == null) {
            return null;
        }
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }

    private LocalDateTime date2LocalDateTime(Date date) {
        if(date == null) {
            return null;
        }
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    }
}