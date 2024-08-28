package com.tapdata.tm.apiCalls.service;

import cn.hutool.core.bean.BeanUtil;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.apiCalls.dto.ApiCallDto;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.vo.ApiCallDetailVo;
import com.tapdata.tm.apicallminutestats.dto.ApiCallMinuteStatsDto;
import com.tapdata.tm.apicallminutestats.service.ApiCallMinuteStatsService;
import com.tapdata.tm.apicallstats.dto.ApiCallStatsDto;
import com.tapdata.tm.apicallstats.service.ApiCallStatsService;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.utils.EntityUtils;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.DocumentUtils.getLong;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class ApiCallService {
    private final ApiCallMinuteStatsService apiCallMinuteStatsService;
    MongoTemplate mongoOperations;
    ModulesService modulesService;
    ApplicationService applicationService;
    ApiCallStatsService apiCallStatsService;

    public ApiCallService(ApiCallMinuteStatsService apiCallMinuteStatsService) {
        this.apiCallMinuteStatsService = apiCallMinuteStatsService;
    }

    public ApiCallEntity findOne(Query query) {
        return mongoOperations.findOne(query, ApiCallEntity.class);
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
        ApiCallEntity apiCallEntity = mongoOperations.findById(id, ApiCallEntity.class);

        apiCallDetailVo = BeanUtil.copyProperties(apiCallEntity, ApiCallDetailVo.class);
        if (apiCallEntity != null && StringUtils.isNotBlank(apiCallEntity.getAllPathId())) {
            ModulesDto modulesDto = modulesService.findById(MongoUtils.toObjectId(apiCallEntity.getAllPathId()));
            if (null != modulesDto) {
                apiCallDetailVo.setName(modulesDto.getName());

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

    public Page<ApiCallDetailVo> find(Filter filter, UserDetail userDetail) {
        Where where = filter.getWhere();
        List orList = (List) where.getOrDefault("or", new ArrayList<>());
        String method = (String) where.getOrDefault("method", "");
        Object code = where.get("code");
        String id = getValueFromOrList(orList, "id");
        String name = getValueFromOrList(orList, "name");
        String order = (String) ((filter.getOrder() == null) ? "createTime DESC" : filter.getOrder());


        Query query = new Query();
        Criteria criteria = new Criteria();


        //先查出这个用户名下能看到的apiCall
        List<ModulesDto> modulesDtoList = new ArrayList<>();
        List<ApiCallEntity> currentUserApiCallList = new ArrayList<>();
        List<String> currentUserApiCallId = new ArrayList<>();
//        Map<ObjectId, ModulesDto> moduleIdToModule = new HashMap<>();

        Criteria noPathIdCriteria = new Criteria();
        if ("admin@admin.com".equals(userDetail.getEmail())) {
            //管理员可以看到所有的访问记录
            List<ModulesDto> deletedModulesDtoList = modulesService.findAll(Query.query(Criteria.where("is_deleted").is(true)));
            criteria.and("allPathId").nin(deletedModulesDtoList);
//            moduleIdToModule=  modulesDtoList.stream().collect(Collectors.toMap(ModulesDto::getId, a -> a, (k1, k2) -> k1));
        } else {
            modulesDtoList = modulesService.getByUserId(userDetail.getUserId());
            if (CollectionUtils.isNotEmpty(modulesDtoList)) {
                currentUserApiCallList = findByUser(modulesDtoList);
                if (CollectionUtils.isNotEmpty(currentUserApiCallList)) {
                    currentUserApiCallId = currentUserApiCallList.stream().map(ApiCallEntity::getId).collect(Collectors.toList())
                            .stream().map(ObjectId::toString).collect(Collectors.toList());
                    criteria.and("id").in(currentUserApiCallId);
//            moduleIdToModule=  modulesDtoList.stream().collect(Collectors.toMap(ModulesDto::getId, a -> a, (k1, k2) -> k1));
                }
            }
        }


        /*组装ID和NAME 的查询条件，比较复杂，后期最好前后端一起优化掉这种传参方式*/
        List<Criteria> nameOrIdCriteriaList = new ArrayList<>();

        //如果要根据name  查找
        List<ModulesEntity> nameModulesList = new ArrayList<>();
        if (StringUtils.isNotEmpty(name)) {
            Query queryModule = Query.query(Criteria.where("user_id").is(userDetail.getUserId()).and("is_deleted").ne(true).and("name").regex(name));
            nameModulesList = mongoOperations.find(queryModule, ModulesEntity.class);
            if (CollectionUtils.isNotEmpty(nameModulesList)) {
                List<String> nameAllPathID = nameModulesList.stream().map(ModulesEntity::getId).collect(Collectors.toList())
                        .stream().map(ObjectId::toString).collect(Collectors.toList());
                nameOrIdCriteriaList.add(Criteria.where("allPathId").in(nameAllPathID));
            }
        }
        //如果要根据 id  查找
        if (StringUtils.isNotEmpty(id)) {
            nameOrIdCriteriaList.add(Criteria.where("_id").is(id));
        }
        Criteria nameOrIdCriteria = new Criteria();
        if (CollectionUtils.isNotEmpty(nameOrIdCriteriaList)) {
            nameOrIdCriteria = new Criteria().orOperator(nameOrIdCriteriaList);
        }
        /*组装ID和NAME 的查询条件，比较复杂，后期最好前后端一起优化掉这种传参方式*/


        //根据method 查询
        if (StringUtils.isNotEmpty(method)) {
            criteria.and("method").is(method);
        }

        if (null != code) {
            if (" ".equals(code)) {
                criteria.and("code").ne("200");
            } else {
                criteria.and("code").is(code);
            }
        }

        Criteria startTimeCriteria = new Criteria();
        if (null != where.get("start")) {
            Double startDate = (Double) where.remove("start");
            startTimeCriteria.and("createAt").gte(new Date(startDate.longValue()));
        }

        Criteria endTimeCriteria = new Criteria();
        if (null != where.get("end")) {
            Double endDate = (Double) where.remove("end");
            endTimeCriteria.and("createAt").lte(new Date(endDate.longValue()));
        }
        criteria.andOperator(startTimeCriteria, endTimeCriteria, nameOrIdCriteria);
        /*---------------------------------------------关联查询条件结束--------------------------------------------*/

        query.addCriteria(criteria);


        TmPageable tmPageable = new TmPageable();
        Integer page = (filter.getSkip() / filter.getLimit()) + 1;
        tmPageable.setPage(page);
        tmPageable.setSize(filter.getLimit());

        if ("createTime DESC".equals(order)) {
            tmPageable.setSort(Sort.by("createTime").descending());
        } else if ("createTime ASC".equals(order)) {
            tmPageable.setSort(Sort.by("createTime").ascending());
        }

        Long total = mongoOperations.count(query, ApiCallEntity.class);
        List<ApiCallEntity> apiCallEntityList = mongoOperations.find(query.with(tmPageable), ApiCallEntity.class);
        List<ApiCallDetailVo> apiCallDetailVoList = new ArrayList<>();


        List<Map> userInfoList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(apiCallEntityList)) {
            userInfoList = apiCallEntityList.stream().map(ApiCallEntity::getUserInfo).collect(Collectors.toList());
        }
        List<String> clientIdList = new ArrayList<>();
        for (Map userInfo : userInfoList) {
            if (null != userInfo.get("clientId")) {
                clientIdList.add((String) userInfo.get("clientId"));
            }
        }

        List<ApplicationDto> applicationDtoList = applicationService.findByIds(clientIdList);
        Map<ObjectId, ApplicationDto> clientIdToApplication = new HashMap<>();
        if (CollectionUtils.isNotEmpty(applicationDtoList)) {
            clientIdToApplication = applicationDtoList.stream().collect(Collectors.toMap(ApplicationDto::getId, a -> a, (k1, k2) -> k1));
        }

        //查询api名称
        List<String> allPathIdList = apiCallEntityList.stream().filter(apiCallEntity -> StringUtils.isNotEmpty(apiCallEntity.getAllPathId())).collect(Collectors.toList())
                .stream().map(ApiCallEntity::getAllPathId).collect(Collectors.toList());
        List<ModulesDto> hitModuledtoList = modulesService.findAll(Query.query(Criteria.where("id").in(allPathIdList)));
        Map<ObjectId, ModulesDto> moduleIdToModule = new HashMap<>();
        if (CollectionUtils.isNotEmpty(hitModuledtoList)) {
            moduleIdToModule = hitModuledtoList.stream().collect(Collectors.toMap(ModulesDto::getId, a -> a, (k1, k2) -> k1));
        }


        for (ApiCallEntity apiCallEntity : apiCallEntityList) {
            String allPathId = apiCallEntity.getAllPathId();
            ApiCallDetailVo apiCallDetailVo = BeanUtil.copyProperties(apiCallEntity, ApiCallDetailVo.class);

            Map userInfo = apiCallEntity.getUserInfo();
            if (null != userInfo && null != userInfo.get("clientId")) {
                String clientId = (String) userInfo.getOrDefault("clientId", "");
                ApplicationDto applicationDto = clientIdToApplication.get(MongoUtils.toObjectId(clientId));
                if (applicationDto != null) {
                    apiCallDetailVo.setClientName(applicationDto.getClientId());
                }
            }
            if (StringUtils.isNotEmpty(allPathId) && null != moduleIdToModule.get(MongoUtils.toObjectId(allPathId))) {
                apiCallDetailVo.setName(moduleIdToModule.get(MongoUtils.toObjectId(allPathId)).getName());
            }

            apiCallDetailVo.setCodeMsg(apiCallEntity.getCodeMsg());
            apiCallDetailVo.setMethod(apiCallEntity.getMethod());
            apiCallDetailVo.setCreateTime(apiCallEntity.getCreateAt());
            apiCallDetailVo.setCode(apiCallEntity.getCode());
            apiCallDetailVo.setUserIp(apiCallEntity.getUserIp());

            apiCallDetailVoList.add(apiCallDetailVo);
        }

        Page result = new Page();
        result.setItems(apiCallDetailVoList);
        result.setTotal(total);
        return result;
    }

    public List<ApiCallDto> save(List<ApiCallDto> saveApiCallParamList) {
        List<ApiCallDto> result = new ArrayList<>();
        List<ApiCallEntity> apiCallEntityList = new ArrayList<>();
        saveApiCallParamList.forEach(saveApiCallParam -> {
            ApiCallEntity apiCallEntity = BeanUtil.copyProperties(saveApiCallParam, ApiCallEntity.class);
            apiCallEntity.setCreateAt(new Date());
            apiCallEntityList.add(apiCallEntity);
        });
        mongoOperations.insert(apiCallEntityList, "ApiCall");

        result = com.tapdata.tm.utils.BeanUtil.deepCloneList(apiCallEntityList, ApiCallDto.class);

        return result;
    }

    public ApiCallDto findOne(Filter filter, UserDetail loginUser) {
        return null;
    }

    public Map<String, List<ApiCallEntity>> getVisitTotalCount(List<ApiCallEntity> apiCallEntityList) {
        return apiCallEntityList.stream().collect(Collectors.groupingBy(item -> {
            if ("200".equals(item.getCode())) {
                return "normal";
            } else {
                return "warning";
            }
        }));
    }

    public List<ApiCallEntity> findByModuleIds(List<String> moduleIdList) {
        Query query = Query.query(Criteria.where("allPathId").in(moduleIdList));
        query.with(Sort.by("createTime").descending());
        List<ApiCallEntity> apiCallEntityList = new ArrayList<>();
        apiCallEntityList = mongoOperations.find(query, ApiCallEntity.class);
        return apiCallEntityList;
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
        return apiCallEntityList;
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

    public List<ApiCallEntity> findByModuleIdAndTimePeriod(List<String> moduleIds, Date startTime, Date endTime) {
        Criteria startTimeCri = Criteria.where("createTime").gte(startTime);
        Criteria endTimeCri = Criteria.where("createTime").lte(endTime);
        Query query = Query.query(Criteria.where("allPathId").in(moduleIds).andOperator(startTimeCri, endTimeCri));
        query.with(Sort.by("createTime").descending());
        List<ApiCallEntity> apiCallEntityList = new ArrayList<>();
        apiCallEntityList = mongoOperations.find(query, ApiCallEntity.class);
        return apiCallEntityList;
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
        ));
        Document group = new Document("_id", groupByMinute())
                .append("responseDataRowTotalCount", new Document("$sum", "$res_rows"))
                .append("totalResponseTime", new Document("$sum", "$latency"))
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
                // responseTimePerRow, rowPerSecond
                apiCallMinuteStatsService.calculate(apiCallMinuteStatsDto);
                apiCallMinuteStatsDto.setLastApiCallId(document.getObjectId("lastApiCallId").toString());
                // apiCallTime: year, month, day, hour, minute
                Document id = document.get("_id", Document.class);
                Instant apiCallTime = LocalDateTime.of(id.getInteger("year"), id.getInteger("month"), id.getInteger("day"), id.getInteger("hour"), id.getInteger("minute")).toInstant(ZoneOffset.UTC);
                apiCallMinuteStatsDto.setApiCallTime(Date.from(apiCallTime));

                apiCallMinuteStatsDtoList.add(apiCallMinuteStatsDto);
            }
        }

        return apiCallMinuteStatsDtoList;
    }

    private Document groupByMinute() {
        return new Document("year", "$year")
                .append("month", "$month")
                .append("day", "$day")
                .append("hour", "$hour")
                .append("minute", "$minute");
    }
}