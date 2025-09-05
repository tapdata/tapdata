package com.tapdata.tm.apiCalls.service;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.apiCalls.dto.ApiCallDto;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.utils.PercentileCalculator;
import com.tapdata.tm.apiCalls.vo.ApiCallDataVo;
import com.tapdata.tm.apiCalls.vo.ApiCallDetailVo;
import com.tapdata.tm.apiCalls.vo.ApiPercentile;
import com.tapdata.tm.apicallminutestats.dto.ApiCallMinuteStatsDto;
import com.tapdata.tm.apicallminutestats.service.ApiCallMinuteStatsService;
import com.tapdata.tm.apicallstats.dto.ApiCallStatsDto;
import com.tapdata.tm.apicallstats.service.ApiCallStatsService;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.ApplicationConfig;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.dto.Param;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.entity.Path;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.system.api.service.TextEncryptionRuleService;
import com.tapdata.tm.system.api.utils.TextEncryptionUtil;
import com.tapdata.tm.utils.EntityUtils;
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

import static com.tapdata.tm.utils.DocumentUtils.getLong;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.limit;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.project;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.skip;

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

    protected Criteria genericFilterCriteria(Filter filter) {
        final Criteria startTimeCriteria = new Criteria();
        final Criteria endTimeCriteria = new Criteria();
        final Where where = filter.getWhere();
        final String method = (String) where.getOrDefault(Tag.METHOD, "");
        final Object code = where.get("code");
        final Object clientId = where.get(Tag.CLIENT_ID);
        final Criteria criteria = new Criteria();
        if (StringUtils.isNotEmpty(method)) {
            criteria.and(Tag.METHOD).is(method);
        }
        if (null != clientId && StringUtils.isNotBlank(String.valueOf(clientId).trim())) {
            criteria.and("user_info").exists(true)
                    .and(String.format("user_info.%s", Tag.CLIENT_ID)).is(clientId);
        }
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

    protected void startFilterApiNameOrId(Filter filter, Criteria criteria) {
        final Where where = filter.getWhere();
        final List<Map<String, Map<String, String>>> orList = (List<Map<String, Map<String, String>>>) where.getOrDefault("or", new ArrayList<>());
        final String id = getValueFromOrList(orList, Tag.ID);
        final String name = getValueFromOrList(orList, Tag.NAME);
        ObjectId apiId = MongoUtils.toObjectId(id);
        if (null != apiId) {
            //filter by api id
            criteria.and(Tag.ALL_PATH_ID).is(id);
        } else if (StringUtils.isNotBlank(name)) {
            //filter by api name
            Criteria regexName = Criteria.where(Tag.NAME).regex(name, "i");
            Query query = Query.query(regexName);
            List<ModulesDto> all = modulesService.findAll(query);
            List<Criteria> or = new ArrayList<>();
            if (!all.isEmpty()) {
                List<String> apiIds = all.stream()
                        .filter(Objects::nonNull)
                        .map(ModulesDto::getId)
                        .map(ObjectId::toString)
                        .distinct()
                        .toList();
                or.add(Criteria.where(Tag.ALL_PATH_ID).in(apiIds));
            }
            if (StringUtils.isNotBlank(id)) {
                or.add(Criteria.where(Tag.ALL_PATH_ID).regex(id));
            }
            if (or.isEmpty()) {
                criteria.and(Tag.ALL_PATH_ID).nin("", null);
            } else {
                criteria.orOperator(or);
            }
        } else {
            criteria.and(Tag.ALL_PATH_ID).nin("", null);
        }
    }

    public Page<ApiCallDetailVo> find(Filter filter) {
        final Where where = filter.getWhere();
        final Object clientId = where.get(Tag.CLIENT_ID);
        final String order = (String) ((filter.getOrder() == null) ? "createTime DESC" : filter.getOrder());

        //filter by method
        final Criteria criteria = genericFilterCriteria(filter);
        startFilterApiNameOrId(filter, criteria);
        AggregationOperation countStage = Aggregation.count().as("total");
        Set<String> clientIds = new HashSet<>();
        if (null != clientId && StringUtils.isNotBlank(String.valueOf(clientId).trim())) {
            clientIds.add(String.valueOf(clientId).trim());
        }
        AggregationOperation matchStage = Aggregation.match(criteria);
        Aggregation countAggregation = Aggregation.newAggregation(
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
                    matchStage,
                    new SortOperation(sort),
                    skip(skip),
                    limit(size),
                    project()
                            .and("_id").as(Tag.ID)
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
            );
            Set<String> moduleIds = new HashSet<>();
            AggregationResults<ApiCallDataVo> apiCall = mongoOperations.aggregate(aggregation, Tag.API_CALL, ApiCallDataVo.class);
            apiCallDetailVoList = Optional.ofNullable(apiCall.getMappedResults()).orElse(new ArrayList<>());
            apiCallDetailVoList.stream()
                    .filter(Objects::nonNull)
                    .map(ApiCallDataVo::getUserInfo)
                    .filter(Objects::nonNull)
                    .filter(e -> Objects.nonNull(e.get(Tag.CLIENT_ID)))
                    .map(e -> String.valueOf(e.get(Tag.CLIENT_ID)))
                    .filter(StringUtils::isNotBlank)
                    .forEach(clientIds::add);
            apiCallDetailVoList.stream()
                    .filter(Objects::nonNull)
                    .map(ApiCallDataVo::getApiId)
                    .filter(Objects::nonNull)
                    .filter(StringUtils::isNotBlank)
                    .forEach(moduleIds::add);
            final Map<String, String> applicationNameMap = new HashMap<>();
            if (!clientIds.isEmpty()) {
                List<ApplicationDto> applications = applicationService.findByIds(new ArrayList<>(clientIds));
                applicationNameMap.putAll(applications.stream()
                        .filter(Objects::nonNull)
                        .filter(e -> Objects.nonNull(e.getClientId()))
                        .collect(Collectors.toMap(ApplicationDto::getClientId, ApplicationDto::getClientName, (e1, e2) -> e2)));
            }
            final Map<String, ModulesDto> modulesDtoMap = new HashMap<>();
            if (!moduleIds.isEmpty()) {
                List<ModulesDto> allModulesByIds = modulesService.findAllModulesByIds(new ArrayList<>(moduleIds));
                modulesDtoMap.putAll(allModulesByIds.stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toMap(e -> e.getId().toHexString(), e -> e, (e1, e2) -> e2)));
            }
            apiCallDetailVoList.stream().filter(Objects::nonNull).forEach(e -> {
                Optional.ofNullable(e.getUserInfo())
                        .map(userInfo -> userInfo.get(Tag.CLIENT_ID))
                        .map(applicationNameMap::get)
                        .ifPresent(e::setClientName);
                Optional.ofNullable(modulesDtoMap.get(e.getApiId())).ifPresent(api -> {
                    e.setApiName(api.getName());
                    e.setApiVersion(api.getApiVersion());
                    e.setApiPath(api.getPath());
                    e.setApiType(api.getApiType());
                    e.setPaths(api.getPaths());
                    e.setProject(api.getProject());
                    e.setConnection(api.getConnection());
                    e.setUser(api.getUser());
                    e.setResRows(api.getResRows());
                    e.setResponseTime(api.getResponseTime());
                    e.setOperationType(api.getOperationType());
                    e.setApiCreateAt(api.getCreateAt());
                });
            });
        } else {
            apiCallDetailVoList = new ArrayList<>();
        }
        final List<ApiCallDetailVo> resultData = Optional.of(apiCallDetailVoList)
                .map(this::afterFindDto)
                .orElse(new ArrayList<>())
                .stream()
                .filter(Objects::nonNull)
                .map(this::mapToApiCallDetailVo)
                .toList();
        return Page.page(resultData, total);
    }

    protected ApiCallDetailVo mapToApiCallDetailVo(ApiCallDataVo e) {
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
        item.setQuery(e.getQuery());
        item.setBody(e.getBody());
        item.setApiPath(e.getApiPath());
        item.setCreateTime(e.getCreateTime());
        item.setCreateAt(e.getApiCreateAt());
        item.setMethod(e.getMethod());
        return item;
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
        Query query = Query.query(Criteria.where(Tag.ALL_PATH_ID).in(moduleIdList));
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
        apiCallEntityList = mongoOperations.find(Query.query(Criteria.where(Tag.ALL_PATH_ID).in(moduleIdList)), ApiCallEntity.class);
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
            map.put(Tag.ID, applicationDto.getId().toString());
            map.put(Tag.NAME, applicationDto.getName());
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
        Document match = new Document(Tag.ALL_PATH_ID, allPathId);
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

    public ApiPercentile getApiPercentile(String apiId, Long from, Long end) {
        if (StringUtils.isBlank(apiId)) {
            throw new BizException("api.call.api.id.required");
        }
        long now = System.currentTimeMillis();
        end = Optional.ofNullable(end).orElse(now);
        end = Math.min(end, now);
        from = Optional.ofNullable(from).orElse(end - 60 * 60 * 1000L);
        from = Math.min(from, end);
        if (end - from > 7 * 24 * 60 * 60 * 1000L) {
            throw new BizException("api.call.percentile.time.range.too.large");
        }
        ApiPercentile apiPercentile = new ApiPercentile();
        ModulesEntity module = mongoOperations.findOne(Query.query(Criteria.where("_id").is(MongoUtils.toObjectId(apiId))).limit(1), ModulesEntity.class);
        if (null == module) {
            return apiPercentile;
        }
        Criteria criteria = Criteria.where("allPathId").is(apiId)
                .and("latency").exists(true)
                .orOperator(
                        new Criteria().andOperator(Criteria.where("reqTime").gte(from), Criteria.where("reqTime").lte(end)),
                        new Criteria().andOperator(Criteria.where("report_time").gte(from), Criteria.where("report_time").lte(end))
                ).and(Tag.DELETE).ne(true);
        Query query = Query.query(criteria);
        query.fields().include("latency");
        List<ApiCallEntity> apiCalls = mongoOperations.find(query, ApiCallEntity.class, "ApiCall");
        List<Long> latencies = apiCalls.stream()
                .filter(Objects::nonNull)
                .map(ApiCallEntity::getLatency)
                .filter(Objects::nonNull)
                .toList();
        if (latencies.isEmpty()) {
            return apiPercentile;
        }
        PercentileCalculator.PercentileResult percentileResult = PercentileCalculator.calculatePercentiles(latencies);
        apiPercentile.setP50(percentileResult.getP50());
        apiPercentile.setP95(percentileResult.getP95());
        apiPercentile.setP99(percentileResult.getP99());
        return apiPercentile;
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
        Map<String, Param> paramMap = findApiParamTypeMap(MongoUtils.toObjectId(entity.getAllPathId())).get(entity.getAllPathId());
        String query = entity.getQuery();
        String body = entity.getBody();
        entity.setQuery(parse(query, open, paramMap));
        entity.setBody(parse(body, open, paramMap));
        entity.setReqParams(null);
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
        Map<String, Map<String, Param>> apiParamTypeMap = findApiParamTypeMap(apiIds.stream().map(ObjectId::new).toArray(ObjectId[]::new));
        entities.stream()
                .filter(Objects::nonNull)
                .forEach(data -> {
            String query = data.getQuery();
            String body = data.getBody();
            Map<String, Param> paramMap = apiParamTypeMap.get(data.getApiId());
            data.setQuery(parse(query, open, paramMap));
            data.setBody(parse(body, open, paramMap));
            data.setReqParams(null);
        });
        return entities;
    }

    protected Map<String, Map<String, Param>> findApiParamTypeMap(ObjectId ...apiId) {
        if (apiId.length == 0) {
            return new HashMap<>();
        }
        Query query = Query.query(Criteria.where("_id").in(new ArrayList<>(Arrays.asList(apiId))));
        query.fields().include("paths.params", "_id");
        List<ModulesDto> all = modulesService.findAll(query);
        return all.stream()
                .filter(Objects::nonNull)
                .filter(e -> Objects.nonNull(e.getId()))
                .filter(e -> Objects.nonNull(e.getPaths())
                        && !e.getPaths().isEmpty()
                        && e.getPaths().get(0) != null
                        && Objects.nonNull(e.getPaths().get(0).getParams())
                        && !e.getPaths().get(0).getParams().isEmpty()
                ).collect(
                        Collectors.toMap(
                                e -> e.getId().toHexString(),
                                e -> {
                                    List<Path> paths = e.getPaths();
                                    Path path = paths.get(0);
                                    return path.getParams().stream()
                                            .filter(Objects::nonNull)
                                            .filter(p -> Objects.nonNull(p.getName()))
                                            .filter(p -> Objects.nonNull(p.getType()))
                                            .collect(Collectors.toMap(Param::getName, p -> p, (p1, p2) -> p2));

                                },
                                (e1, e2) -> e2
                        )
                );
    }

    String parse(String json, Boolean open, Map<String, Param> paramMap) {
        if (null == json) {
            return null;
        }
        try {
            Map<String,Object> map = JSON.parseObject(json, Map.class);
            TextEncryptionUtil.formatBefore(map, paramMap);
            if (!Boolean.TRUE.equals(open)) {
                return JSON.toJSONString(map);
            }
            return JSON.toJSONString(TextEncryptionUtil.textEncryptionBySwitch(map));
        } catch (Exception e) {
            log.warn("Parse json failed, can not encrypt by config: {}, json: {}, msg: {}", open, json, e.getMessage());
            return json;
        }
    }

    public static class Tag {
        private Tag() {}
        public static final String CREATE_TIME = "createTime";
        public static final String METHOD = "method";
        public static final String CLIENT_ID = "clientId";
        public static final String START = "start";
        public static final String ALL_PATH_ID = "allPathId";
        public static final String API_CALL = "ApiCall";
        public static final String CREATE_AT = "createAt";
        public static final String LATENCY = "latency";
        public static final String NAME = "name";
        public static final String ID = "id";
        public static final String DELETE = "delete";
    }
}