package com.tapdata.tm.apiCalls.service;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UnwindOptions;
import com.tapdata.tm.apiCalls.dto.ApiCallDto;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.vo.ApiCallDataVo;
import com.tapdata.tm.apiCalls.vo.ApiCallDetailVo;
import com.tapdata.tm.apicallminutestats.dto.ApiCallMinuteStatsDto;
import com.tapdata.tm.apicallminutestats.service.ApiCallMinuteStatsService;
import com.tapdata.tm.apicallstats.dto.ApiCallStatsDto;
import com.tapdata.tm.apicallstats.service.ApiCallStatsService;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.TmPageable;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.ApplicationConfig;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.system.api.dto.TextEncryptionRuleDto;
import com.tapdata.tm.system.api.service.TextEncryptionRuleService;
import com.tapdata.tm.system.api.utils.TextEncryptionUtil;
import com.tapdata.tm.utils.EntityUtils;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.tapdata.tm.utils.DocumentUtils.getLong;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.limit;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.project;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.skip;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.unwind;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class ApiCallService {
    ApiCallMinuteStatsService apiCallMinuteStatsService;
    MongoTemplate mongoOperations;
    ModulesService modulesService;
    ApplicationService applicationService;
    ApiCallStatsService apiCallStatsService;
    protected ApplicationConfig applicationConfig;
    protected TextEncryptionRuleService ruleService;

    public ApiCallService() {
    }

    public ApiCallEntity findOne(Query query) {
        return Optional.ofNullable(mongoOperations.findOne(query, ApiCallEntity.class))
                .map(this::afterFindEntity)
                .orElse(null);
    }

    public ApiCallDto upsertByWhere(Where where, ApiCallDto metadataDefinition, UserDetail loginUser) {
        return null;
    }

    public Long updateByWhere(Where where, ApiCallDto metadataDefinition, UserDetail loginUser) {
        return null;
    }

    public void deleteLogicsById(String id) {
    }

    public ApiCallDetailVo findById(String id, Field fields, UserDetail loginUser) {
        ApiCallDetailVo apiCallDetailVo = new ApiCallDetailVo();
        ApiCallEntity apiCallEntity = Optional.ofNullable(mongoOperations.findById(id, ApiCallEntity.class))
                .map(this::afterFindEntity)
                .orElse(null);

        apiCallDetailVo = BeanUtil.copyProperties(apiCallEntity, ApiCallDetailVo.class);
        if (apiCallEntity != null && StringUtils.isNotBlank(apiCallEntity.getAllPathId())) {
            ModulesDto modulesDto = modulesService.findById(MongoUtils.toObjectId(apiCallEntity.getAllPathId()));
            if (null != modulesDto) {
                apiCallDetailVo.setName(modulesDto.getName());
                apiCallDetailVo.setApiId(apiCallEntity.getAllPathId());
                apiCallDetailVo.setApiPath(apiCallEntity.getReq_path());

                //
                List<ApiCallEntity> apiCallEntityList = findByModuleIds(Arrays.asList(modulesDto.getId().toString()));
                //计算平均耗时
                if (CollectionUtils.isNotEmpty(apiCallEntityList)) {
                    Double totalReqRows = apiCallEntityList.stream().filter(item -> null != item.getResRows()).collect(Collectors.toList()).stream().mapToDouble(ApiCallEntity::getResRows).sum();
                    apiCallDetailVo.setVisitTotalCount(totalReqRows.longValue());

                    double totalReqByte = apiCallEntityList.stream().mapToDouble(ApiCallEntity::getReqBytes).sum();
                    //要转 成秒
                    double totalLatency = apiCallEntityList.stream().mapToDouble(ApiCallEntity::getLatency).sum();
                    if (totalLatency > 0) {
                        apiCallDetailVo.setSpeed((long) ((totalReqByte / totalLatency) * 1000));
                    }

                    if (totalReqRows > 0) {
                        apiCallDetailVo.setAverResponseTime((long) (totalLatency / totalReqRows));
                    }

                    apiCallDetailVo.setLatency((long) (totalLatency / apiCallEntityList.size()));
                }
            }
        }
        return apiCallDetailVo;
    }

    public ApiCallDto updateById(ApiCallDto metadataDefinition, UserDetail userDetail) {
        return null;
    }

    protected List<Document> buildNameOrIdExpr(String name, String id, UserDetail userDetail) {
        List<Document> matchCriteria = new ArrayList<>();
        matchCriteria.add(new Document("$eq", List.of("$_id", new Document("$toObjectId", "$$allPathId"))));
        matchCriteria.add(new Document("$ne", List.of("$delete", true)));
        if (!Objects.equals(applicationConfig.getAdminAccount(), userDetail.getEmail())) {
            matchCriteria.add(new Document("$eq", List.of("$user_id", userDetail.getUserId())));
        }
        ObjectId objectId = MongoUtils.toObjectId(id);
        //前端都是放在一个输入框，如果是ObjectID就是查询apiId,否则就是模糊查询api name
        if (null != objectId) {
            matchCriteria.add(new Document("$eq", List.of("$_id", objectId)));
        } else if (StringUtils.isNotBlank(name)) {
            matchCriteria.add(new Document("$regexMatch", new Document("input", "$name").append("regex", name)));
        }
        return matchCriteria;
    }

    protected Criteria genericFilterCriteria(Filter filter) {
        final Criteria startTimeCriteria = new Criteria();
        final Criteria endTimeCriteria = new Criteria();
        final Where where = filter.getWhere();
        final String method = (String) where.getOrDefault(Tag.METHOD, "");
        final Object code = where.get("code");
        final Criteria criteria = new Criteria();
        if (StringUtils.isNotEmpty(method)) {
            criteria.and(Tag.METHOD).is(method);
        }
        criteria.and(Tag.ALL_PATH_ID).nin("", null);
        Optional.ofNullable(code)
                .map(value -> String.valueOf(value).trim())
                .ifPresent(value -> {
                    if (Objects.equals("", value)) {
                        criteria.and("code").ne("200");
                    } else {
                        criteria.and("code").is(value);
                    }
                });
        Optional.ofNullable(where.get(Tag.START))
                .map(value -> (Double) where.remove(Tag.START))
                .map(value -> new Date(value.longValue()))
                .ifPresent(value -> startTimeCriteria.and(Tag.CREATE_TIME).gte(value));
        Optional.ofNullable(where.get("end"))
                .map(value -> (Double) where.remove("end"))
                .map(value -> new Date(value.longValue()))
                .ifPresent(value -> endTimeCriteria.and(Tag.CREATE_TIME).lte(value));
        criteria.andOperator(startTimeCriteria, endTimeCriteria);
        return criteria;
    }

    public Page<ApiCallDetailVo> find(Filter filter, UserDetail userDetail) {
        final Where where = filter.getWhere();
        final List<Map<String, Map<String, String>>> orList = (List<Map<String, Map<String, String>>>) where.getOrDefault("or", new ArrayList<>());
        final Object clientId = where.get("clientId");
        final String id = getValueFromOrList(orList, "id");
        final String name = getValueFromOrList(orList, "name");
        final String order = (String) ((filter.getOrder() == null) ? "createTime DESC" : filter.getOrder());

        //根据method 查询
        final Criteria criteria = genericFilterCriteria(filter);
        List<Document> bMatchCriteria = buildNameOrIdExpr(name, id, userDetail);
        Document lookupDoc = new Document("$lookup", new Document()
                .append("from", "Modules")
                .append("let", new Document(Tag.ALL_PATH_ID, "$" + Tag.ALL_PATH_ID))
                .append("pipeline", List.of(new Document(Tag.MATCH, new Document("$expr", new Document("$and", bMatchCriteria)))))
                .append("as", "mData")
        );
        AggregationOperation lookupOfModule = context -> lookupDoc;
        Criteria criteriaAll = Criteria.where("mData.0").exists(true).andOperator(criteria);

        AggregationOperation countStage = Aggregation.count().as("total");


        List<Document> filterApplication = new ArrayList<>();
        filterApplication.add(new Document("$eq", List.of("$clientId", "$$clientId")));
        if (null != clientId && StringUtils.isNotBlank(String.valueOf(clientId).trim())) {
            filterApplication.add(new Document("$eq", List.of("$clientId", clientId)));
            criteriaAll.and("appData.clientId").is(String.valueOf(clientId).trim());
        }
        Document lookupDocOfApplication = new Document("$lookup", new Document()
                .append("from", "Application")
                .append("let", new Document("clientId", "$user_info.clientId"))
                .append("pipeline", List.of(new Document(Tag.MATCH, new Document("$expr", new Document("$and", filterApplication)))))
                .append("as", "appData")
        );
        AggregationOperation lookupOfApplication = context -> lookupDocOfApplication;

        AggregationOperation matchStage = Aggregation.match(criteriaAll);
        Aggregation countAggregation = Aggregation.newAggregation(
                match(criteria),
                lookupOfApplication,
                lookupOfModule,
                matchStage,
                countStage
        );
        final AggregationResults<Map<String, Number>> countResults = mongoOperations.aggregate(countAggregation, Tag.API_CALL, (Class<Map<String, Number>>) (Class<?>) Map.class);
        final long total = countResults.getMappedResults().isEmpty() ? 0L : Optional.ofNullable(countResults.getMappedResults().get(0))
                .map(e -> e.get("total"))
                .map(Number::longValue)
                .orElse(0L);
        final List<ApiCallDataVo> apiCallDetailVoList;
        if (total > 0L) {
            final int skip = filter.getSkip();
            final int size = filter.getLimit();
            org.springframework.data.domain.Sort sort;
            if ("createTime ASC".equals(order)) {
                sort = Sort.by(Tag.CREATE_TIME).ascending();
            } else {
                sort = Sort.by(Tag.CREATE_TIME).descending();
            }
            final Aggregation aggregation = newAggregation(
                    match(criteria),
                    lookupOfApplication,
                    lookupOfModule,
                    matchStage,
                    unwind("mData"),
                    unwind("appData", true),
                    new SortOperation(sort),
                    skip(skip),
                    limit(size),
                    project()
                            .and("_id").as("id")
                            .and(Tag.CREATE_TIME).as(Tag.CREATE_AT)
                            .and("user_id").as("userId")
                            .and("createUser").as("createUser")
                            .and(Tag.LATENCY).as(Tag.LATENCY)
                            .and("reqTime").as("reqTime")
                            .and("resTime").as("resTime")
                            .and("api_meta").as("apiMeta")
                            .and("user_info").as("userInfo")
                            .and("call_id").as("callId")
                            .and("user_ip").as("userIp")
                            .and("user_port").as("userPort")
                            .and(Tag.METHOD).as(Tag.METHOD)
                            .and("code").as("code")
                            .and("codeMsg").as("codeMsg")
                            .and("report_time").as("reportTime")
                            .and("visitTotalCount").as("visitTotalCount")
                            .and(Tag.CREATE_TIME).as(Tag.CREATE_TIME)
                            .and("speed").as("speed")
                            .and("averResponseTime").as("averResponseTime")
                            .and("req_params").as("reqParams")
                            .and("query").as("query")
                            .and("body").as("body")
                            .and(Tag.ALL_PATH_ID).as("apiId")
                            .and("req_path").as("apiPath")

                            .and("mData.name").as("apiName")
                            .and("mData.apiVersion").as("apiVersion")
                            .and("mData.path").as("path")
                            .and("mData.apiType").as("apiType")
                            .and("mData.paths").as("paths")
                            .and("mData.project").as("project")
                            .and("mData.connection").as("connection")
                            .and("mData.user").as("user")
                            .and("mData.resRows").as("resRows")
                            .and("mData.responseTime").as("responseTime")
                            .and("mData.operationType").as("operationType")
                            .and("mData.createAt").as("apiCreateAt")

                            .and("appData.clientId").as("clientId")
                            .and("appData.clientName").as("clientName")
            );
            AggregationResults<ApiCallDataVo> apiCall = mongoOperations.aggregate(aggregation, Tag.API_CALL, ApiCallDataVo.class);
            apiCallDetailVoList = Optional.ofNullable(apiCall.getMappedResults()).orElse(new ArrayList<>());
        } else {
            apiCallDetailVoList = new ArrayList<>();
        }
        final List<ApiCallDetailVo> resultData = Optional.of(apiCallDetailVoList)
                .map(this::afterFindDto)
                .orElse(new ArrayList<>())
                .stream()
                .filter(Objects::nonNull)
                .map(e -> {
                    final ApiCallDetailVo item = new ApiCallDetailVo();
                    item.setClientName(e.getClientName());
                    item.setId(e.getId().toHexString());
                    item.setApiId(e.getApiId());
                    item.setName(e.getApiName());
                    item.setCode(e.getCode());
                    item.setLatency(e.getLatency());
                    item.setSpeed(e.getSpeed());
                    item.setCodeMsg(e.getCodeMsg());
                    item.setAverResponseTime(e.getAverResponseTime());
                    item.setVisitTotalCount(e.getVisitTotalCount());
                    item.setUserIp(e.getUserIp());
                    item.setLastUpdAt(e.getLastUpdAt());
                    item.setLastUpdBy(e.getLastUpdBy());
                    item.setCustomId(e.getCustomId());
                    item.setReqParams(e.getReqParams());
                    item.setQuery(e.getQuery());
                    item.setBody(e.getBody());
                    item.setApiPath(e.getApiPath());
                    item.setCreateTime(e.getCreateTime());
                    item.setCreateAt(e.getApiCreateAt());
                    item.setMethod(e.getMethod());
                    return item;
        }).toList();
        return Page.page(resultData, total);
    }

    public List<ApiCallDto> save(List<ApiCallDto> saveApiCallParamList) {
        List<ApiCallEntity> apiCallEntityList = new ArrayList<>();
        saveApiCallParamList.forEach(saveApiCallParam -> {
            ApiCallEntity apiCallEntity = BeanUtil.copyProperties(saveApiCallParam, ApiCallEntity.class);
            apiCallEntity.setCreateAt(new Date());
            apiCallEntityList.add(apiCallEntity);
        });
        mongoOperations.insert(apiCallEntityList, "ApiCall");
        return Optional.of(apiCallEntityList)
                .map(this::afterFindEntity)
                .map(e -> com.tapdata.tm.utils.BeanUtil.deepCloneList(apiCallEntityList, ApiCallDto.class))
                .orElse(new ArrayList<>());
    }

    public ApiCallDto findOne(Filter filter, UserDetail loginUser) {
        return null;
    }

    public List<ApiCallEntity> findByModuleIds(List<String> moduleIdList) {
        Query query = Query.query(Criteria.where("allPathId").in(moduleIdList));
        query.with(Sort.by("createTime").descending());
        List<ApiCallEntity> apiCallEntityList = new ArrayList<>();
        apiCallEntityList = mongoOperations.find(query, ApiCallEntity.class);
        return Optional.of(apiCallEntityList).map(this::afterFindEntity).orElse(null);
    }


    /**
     * 获取某个用户名下的所有请求
     *
     * @param modulesDtoList
     * @return
     */
    public List<ApiCallEntity> findByUser(List<ModulesDto> modulesDtoList) {
        List<ApiCallEntity> apiCallEntityList = new ArrayList<>();
        List<String> moduleIdList = modulesDtoList.stream().map(ModulesDto::getId).collect(Collectors.toList())
                .stream().map(ObjectId::toString).collect(Collectors.toList())
                .stream().distinct().collect(Collectors.toList());
        apiCallEntityList = mongoOperations.find(Query.query(Criteria.where("allPathId").in(moduleIdList)), ApiCallEntity.class);
        return Optional.of(apiCallEntityList).map(this::afterFindEntity).orElse(new ArrayList<>());
    }

    public List<Map<String, String>> findClients(List<String> moduleIdList) {
        List<Map<String, String>> result = new ArrayList<>();
        Query query = new Query();
        if (CollectionUtils.isNotEmpty(moduleIdList)) {
            query.addCriteria(Criteria.where("moduleId").in(moduleIdList));
        }
        query.fields().include("clientIds");
        List<ApiCallStatsDto> apiCallStatsDtoList = apiCallStatsService.findAll(query);
        Set<String> clientIdSet = new HashSet<>();
        apiCallStatsDtoList.forEach(apiCallStatsDto -> {
            Set<String> clientIds = apiCallStatsDto.getClientIds();
            if (CollectionUtils.isNotEmpty(clientIds)) {
                clientIdSet.addAll(clientIds);
            }
        });

        List<ApplicationDto> applicationDtoList = applicationService.findByIds(new ArrayList<>(clientIdSet));
        applicationDtoList.forEach(applicationDto -> {
            Map<String, String> map = new HashMap<>();
            map.put("id", applicationDto.getId().toString());
            map.put("name", applicationDto.getName());
            result.add(map);
        });

        return result;
    }

    private String getValueFromOrList(List<Map<String, Map<String, String>>> orList, String fieldName) {
        String fieldValue = "";
        for (Map<String, Map<String, String>> orMap : orList) {
            for (String key : orMap.keySet()) {
                if (fieldName.equals(key)) {
                    fieldValue = orMap.get(key).get("$regex");
                }
            }
        }
        return fieldValue;
    }

    public ApiCallStatsDto aggregateByAllPathId(String allPathId, String lastApiCallId) {
        if (StringUtils.isBlank(allPathId)) {
            return null;
        }
        ApiCallStatsDto apiCallStatsDto = new ApiCallStatsDto();
        apiCallStatsDto.setModuleId(allPathId);
        String apiCallCollectionName;
        try {
            apiCallCollectionName = EntityUtils.documentAnnotationValue(ApiCallEntity.class);
        } catch (Exception e) {
            throw new BizException("Get ApiCallEntity's collection name failed", e);
        }
        MongoCollection<Document> apiCallCollection = mongoOperations.getCollection(apiCallCollectionName);

        // Build aggregation pipeline
        Document match = new Document("allPathId", allPathId);
        if (StringUtils.isNotBlank(lastApiCallId)) {
            match.append("_id", new Document("$gt", new ObjectId(lastApiCallId)));
        }
        List<Document> pipeline = Arrays.asList(new Document("$match", match),
                new Document("$facet",
                        new Document("callTotalCount", Arrays.asList(new Document("$group", new Document("_id", "$allPathId").append("data", new Document("$sum", 1L)))))
                                .append("transferDataTotalBytes", Arrays.asList(new Document("$group", new Document("_id", "$allPathId").append("data", new Document("$sum", "$req_bytes")))))
                                .append("callAlarmTotalCount", Arrays.asList(new Document("$match", new Document("code", new Document("$ne", "200"))), new Document("$group", new Document("_id", "$allPathId").append("data", new Document("$sum", 1L)))))
                                .append("responseDataRowTotalCount", Arrays.asList(new Document("$group", new Document("_id", "$allPathId").append("data", new Document("$sum", "$res_rows")))))
                                .append("totalResponseTime", Arrays.asList(new Document("$group", new Document("_id", "$allPathId").append("data", new Document("$sum", "$latency")))))
                                .append("lastApiCallId", Arrays.asList(new Document("$project", new Document("_id", 1L)), new Document("$sort", new Document("_id", -1L)), new Document("$limit", 1L)))
                                .append("maxResponseTime", Arrays.asList(new Document("$group", new Document("_id", "$allPathId").append("data", new Document("$max", "$latency")))))
                                .append("clientIds", Arrays.asList(new Document("$group", new Document("_id", "$user_info.clientId"))))
                )
        );
        if (log.isDebugEnabled()) {
            StringBuilder pipelineString = new StringBuilder();
            pipeline.forEach(document -> pipelineString.append(document.toJson()).append(System.lineSeparator()));
            log.debug("ApiCallStatsService.aggregateApiCallStats pipeline: {}{}", System.lineSeparator(), pipelineString);
        }

        // Execute aggregation
        long startMs = System.currentTimeMillis();
        try (
                MongoCursor<Document> iterator = apiCallCollection.aggregate(pipeline, Document.class).allowDiskUse(true).iterator()
        ) {
            if (iterator.hasNext()) {
                // Parse the result of the aggregation, and set value in ApiCallStatsDto
                Document doc = iterator.next();
                long costMs = System.currentTimeMillis() - startMs;
                if (log.isDebugEnabled()) {
                    log.debug("Execute aggregate by module id [{}], get result document: {}, cost: {} ms", allPathId, doc.toJson(), costMs);
                }
                List<?> tempList;
                // callTotalCount
                Object callTotalCount = doc.get("callTotalCount");
                if (callTotalCount instanceof List) {
                    tempList = (List<?>) callTotalCount;
                    if (!tempList.isEmpty()) {
                        apiCallStatsDto.setCallTotalCount(getLong((Document) tempList.get(0), "data"));
                    }
                }
                // transferDataTotalBytes
                Object transferDataTotalBytes = doc.get("transferDataTotalBytes");
                if (transferDataTotalBytes instanceof List) {
                    tempList = (List<?>) transferDataTotalBytes;
                    if (!tempList.isEmpty()) {
                        apiCallStatsDto.setTransferDataTotalBytes(getLong((Document) tempList.get(0), "data"));
                    }
                }
                // callAlarmTotalCount
                Object callAlarmTotalCount = doc.get("callAlarmTotalCount");
                if (callAlarmTotalCount instanceof List) {
                    tempList = (List<?>) callAlarmTotalCount;
                    if (!tempList.isEmpty()) {
                        apiCallStatsDto.setCallAlarmTotalCount(getLong((Document) tempList.get(0), "data"));
                    }
                }
                // responseDataRowTotalCount
                Object responseDataRowTotalCount = doc.get("responseDataRowTotalCount");
                if (responseDataRowTotalCount instanceof List) {
                    tempList = (List<?>) responseDataRowTotalCount;
                    if (!tempList.isEmpty()) {
                        apiCallStatsDto.setResponseDataRowTotalCount(getLong((Document) tempList.get(0), "data"));
                    }
                }
                // totalResponseTime
                Object totalResponseTime = doc.get("totalResponseTime");
                if (totalResponseTime instanceof List) {
                    tempList = (List<?>) totalResponseTime;
                    if (!tempList.isEmpty()) {
                        apiCallStatsDto.setTotalResponseTime(getLong((Document) tempList.get(0), "data"));
                    }
                }
                // maxResponseTime
                Object maxResponseTime = doc.get("maxResponseTime");
                if (maxResponseTime instanceof List) {
                    tempList = (List<?>) maxResponseTime;
                    if (!tempList.isEmpty()) {
                        apiCallStatsDto.setMaxResponseTime(getLong((Document) tempList.get(0), "data"));
                    }
                }
                // lastApiCallId
                Object lastApiCallIdObj = doc.get("lastApiCallId");
                if (lastApiCallIdObj instanceof List) {
                    tempList = (List<?>) lastApiCallIdObj;
                    if (!tempList.isEmpty()) {
                        apiCallStatsDto.setLastApiCallId(((Document) tempList.get(0)).getObjectId("_id").toString());
                    }
                }
                // clientIds
                Object clientIds = doc.get("clientIds");
                if (clientIds instanceof List) {
                    tempList = (List<?>) clientIds;
                    if (!tempList.isEmpty()) {
                        tempList.stream().map(t -> ((Document) t).getString("_id")).filter(Objects::nonNull).forEach(apiCallStatsDto.getClientIds()::add);
                    }
                }
            }
        }

        return apiCallStatsDto;
    }

    public List<ApiCallMinuteStatsDto> aggregateMinuteByAllPathId(String allPathId, String lastApiCallId, Date startTime) {
        List<ApiCallMinuteStatsDto> apiCallMinuteStatsDtoList = new ArrayList<>();
        String apiCallCollectionName;
        try {
            apiCallCollectionName = EntityUtils.documentAnnotationValue(ApiCallEntity.class);
        } catch (Exception e) {
            throw new BizException("Get ApiCallEntity's collection name failed", e);
        }
        MongoCollection<Document> apiCallCollection = mongoOperations.getCollection(apiCallCollectionName);

        // Build aggregation pipeline
        Document match = new Document("allPathId", allPathId);
        if (StringUtils.isNotBlank(lastApiCallId)) {
            match.append("_id", new Document("$gt", new ObjectId(lastApiCallId)));
        }
        if (null != startTime) {
            match.append("createTime", new Document("$gte", startTime));
        }
        List<Document> pipeline = new ArrayList<>();
        pipeline.add(new Document("$match", match));
        pipeline.add(new Document("$project", new Document("year", new Document("$year", "$createTime"))
                .append("month", new Document("$month", "$createTime"))
                .append("day", new Document("$dayOfMonth", "$createTime"))
                .append("hour", new Document("$hour", "$createTime"))
                .append("minute", new Document("$minute", "$createTime"))
                .append("res_rows", 1)
                .append("latency", 1)
                .append("req_bytes", 1)
        ));
        Document group = new Document("_id", groupByMinute())
                .append("responseDataRowTotalCount", new Document("$sum", "$res_rows"))
                .append("totalResponseTime", new Document("$sum", "$latency"))
                .append("transferDataTotalBytes", new Document("$sum", "$req_bytes"))
                .append("lastApiCallId", new Document("$last", "$_id"));
        pipeline.add(new Document("$group", group));
        if (log.isDebugEnabled()) {
            StringBuilder pipelineString = new StringBuilder();
            pipeline.forEach(document -> pipelineString.append(document.toJson()).append(System.lineSeparator()));
            log.debug("ApiCallStatsService.aggregateMinuteByAllPathId pipeline: {}{}", System.lineSeparator(), pipelineString);
        }
        // Execute aggregation
        try (
                MongoCursor<Document> iterator = apiCallCollection.aggregate(pipeline, Document.class).allowDiskUse(true).iterator()
        ) {
            while (iterator.hasNext()) {
                Document document = iterator.next();
                ApiCallMinuteStatsDto apiCallMinuteStatsDto = new ApiCallMinuteStatsDto();
                apiCallMinuteStatsDto.setModuleId(allPathId);

                apiCallMinuteStatsDto.setResponseDataRowTotalCount(getLong(document, "responseDataRowTotalCount"));
                apiCallMinuteStatsDto.setTotalResponseTime(getLong(document, "totalResponseTime"));
                apiCallMinuteStatsDto.setTransferDataTotalBytes(getLong(document, "transferDataTotalBytes"));
                // responseTimePerRow, rowPerSecond
                apiCallMinuteStatsService.calculate(apiCallMinuteStatsDto);
				if (null != document.get("lastApiCallId")) {
					apiCallMinuteStatsDto.setLastApiCallId(document.getObjectId("lastApiCallId").toString());
				}
                // apiCallTime: year, month, day, hour, minute
                Document id = document.get("_id", Document.class);
                Instant apiCallTime = LocalDateTime.of(
						id.getInteger("year"),
						id.getInteger("month"),
						id.getInteger("day"),
						id.getInteger("hour"),
						id.getInteger("minute")
				).toInstant(ZoneOffset.UTC);
                apiCallMinuteStatsDto.setApiCallTime(Date.from(apiCallTime));

                apiCallMinuteStatsDtoList.add(apiCallMinuteStatsDto);
            }
        }

        return apiCallMinuteStatsDtoList;
    }

    public Document groupByMinute() {
        return new Document("year", "$year")
                .append("month", "$month")
                .append("day", "$day")
                .append("hour", "$hour")
                .append("minute", "$minute");
    }

    protected ApiCallEntity afterFindEntity(ApiCallEntity entity) {
        final Boolean open = ruleService.checkAudioSwitchStatus();
        doAfterOnce(open, entity);
        return entity;
    }

    protected void doAfterOnce(Boolean open, ApiCallEntity entity) {
        String query = entity.getQuery();
        String body = entity.getBody();
        String reqParams = entity.getReqParams();
        entity.setQuery(parse(query, open));
        entity.setBody(parse(body, open));
        entity.setReqParams(parse(reqParams, open));
    }

    protected List<ApiCallEntity> afterFindEntity(List<ApiCallEntity> entities) {
        final List<String> apiIds = entities.stream()
                .filter(Objects::nonNull)
                .map(ApiCallEntity::getAllPathId)
                .filter(Objects::nonNull)
                .toList();
        if (apiIds.isEmpty()) {
            return entities;
        }
        final Boolean open = ruleService.checkAudioSwitchStatus();
        entities.forEach(entity -> doAfterOnce(open, entity));
        return entities;
    }

    protected List<ApiCallDataVo> afterFindDto(List<ApiCallDataVo> entities) {
        final List<String> apiIds = entities.stream()
                .filter(Objects::nonNull)
                .map(ApiCallDataVo::getApiId)
                .filter(Objects::nonNull)
                .toList();
        if (apiIds.isEmpty()) {
            return entities;
        }
        final Boolean open = ruleService.checkAudioSwitchStatus();
        entities.forEach(data -> {
            String query = data.getQuery();
            String body = data.getBody();
            String reqParams = data.getReqParams();
            data.setQuery(parse(query, open));
            data.setBody(parse(body, open));
            data.setReqParams(parse(reqParams, open));
        });
        return entities;
    }

    String parse(String json, Boolean open) {
        if (null == json || !Boolean.TRUE.equals(open)) {
            return json;
        }
        try {
            return JSON.toJSONString(TextEncryptionUtil.textEncryptionBySwitch(open, Lists.of(JSON.parseObject(json, Map.class))).get(0));
        } catch (Exception e) {
            log.warn("Parse json failed, can not encrypt by config: {}, json: {}, msg: {}", open, json, e.getMessage());
            return json;
        }
    }

    public static class Tag {
        private Tag() {}
        public static final String MATCH = "$match";
        public static final String CREATE_TIME = "createTime";
        public static final String METHOD = "method";
        public static final String START = "start";
        public static final String ALL_PATH_ID = "allPathId";
        public static final String API_CALL = "ApiCall";
        public static final String CREATE_AT = "createAt";
        public static final String LATENCY = "latency";
    }
}