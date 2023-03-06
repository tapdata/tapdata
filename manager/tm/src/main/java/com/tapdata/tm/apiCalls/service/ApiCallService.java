package com.tapdata.tm.apiCalls.service;

import cn.hutool.core.bean.BeanUtil;
import com.tapdata.tm.apiCalls.dto.ApiCallDto;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.vo.ApiCallDetailVo;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.param.ApiDetailParam;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.utils.MongoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Service
@Slf4j
public class ApiCallService {
    @Autowired
    MongoTemplate mongoOperations;

    @Autowired
    ModulesService modulesService;

    @Autowired
    ApplicationService applicationService;

    public ApiCallDto findOne(Query query) {
        return null;
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
            currentUserApiCallList = findByUser(modulesDtoList);
            currentUserApiCallId = currentUserApiCallList.stream().map(ApiCallEntity::getId).collect(Collectors.toList())
                    .stream().map(ObjectId::toString).collect(Collectors.toList());
            criteria.and("id").in(currentUserApiCallId);
//            moduleIdToModule=  modulesDtoList.stream().collect(Collectors.toMap(ModulesDto::getId, a -> a, (k1, k2) -> k1));
        }


        /*组装ID和NAME 的查询条件，比较复杂，后期最好前后端一起优化掉这种传参方式*/
        List<Criteria> nameOrIdCriteriaList = new ArrayList<>();

        //如果要根据name  查找
        List<ModulesEntity> nameModulesList = new ArrayList<>();
        if (StringUtils.isNotEmpty(name)) {
            Query queryModule = Query.query(Criteria.where("user_id").is(userDetail.getUserId()).and("is_deleted").ne(true).and("name").regex(name));
            nameModulesList = mongoOperations.find(queryModule, ModulesEntity.class);
            List<String> nameAllPathID = nameModulesList.stream().map(ModulesEntity::getId).collect(Collectors.toList())
                    .stream().map(ObjectId::toString).collect(Collectors.toList());
            nameOrIdCriteriaList.add(Criteria.where("allPathId").in(nameAllPathID));
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


        List<Map> userInfoList = apiCallEntityList.stream().map(ApiCallEntity::getUserInfo).collect(Collectors.toList());
        List<String> clientIdList = new ArrayList<>();
        for (Map userInfo : userInfoList) {
            if (null != userInfo.get("clientId")) {
                clientIdList.add((String) userInfo.get("clientId"));
            }
        }

        List<ApplicationDto> applicationDtoList = applicationService.findByIds(clientIdList);
        Map<ObjectId, ApplicationDto> clientIdToApplication = applicationDtoList.stream().collect(Collectors.toMap(ApplicationDto::getId, a -> a, (k1, k2) -> k1));


        //查询api名称
        List<String> allPathIdList = apiCallEntityList.stream().filter(apiCallEntity -> StringUtils.isNotEmpty(apiCallEntity.getAllPathId())).collect(Collectors.toList())
                .stream().map(ApiCallEntity::getAllPathId).collect(Collectors.toList());
        List<ModulesDto> hitModuledtoList = modulesService.findAll(Query.query(Criteria.where("id").in(allPathIdList)));
        Map<ObjectId, ModulesDto> moduleIdToModule = hitModuledtoList.stream().collect(Collectors.toMap(ModulesDto::getId, a -> a, (k1, k2) -> k1));


        for (ApiCallEntity apiCallEntity : apiCallEntityList) {
            String allPathId = apiCallEntity.getAllPathId();
            ApiCallDetailVo apiCallDetailVo = BeanUtil.copyProperties(apiCallEntity, ApiCallDetailVo.class);

            Map userInfo = apiCallEntity.getUserInfo();
            if (null != userInfo && null != userInfo.get("clientId")) {
                String clientId = (String) userInfo.getOrDefault("clientId", "");
                apiCallDetailVo.setClientName(clientIdToApplication.get(MongoUtils.toObjectId(clientId)).getName());
            }
            if (StringUtils.isNotEmpty(allPathId) && null != moduleIdToModule.get(MongoUtils.toObjectId(allPathId))) {
                apiCallDetailVo.setName(moduleIdToModule.get(MongoUtils.toObjectId(allPathId)).getName());
            }

            apiCallDetailVo.setCodeMsg(apiCallEntity.getCodeMsg());
            apiCallDetailVo.setMethod(apiCallEntity.getMethod());
            apiCallDetailVo.setCreateTime(apiCallEntity.getCreateAt());
            apiCallDetailVo.setCode(apiCallEntity.getCode());

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


    /**
     * 获取传输的数据量
     *
     * @return
     */
    public Long getTransmitTotal(List<ApiCallEntity> apiCallEntityList) {
        Long totalScore = apiCallEntityList.stream().mapToLong(ApiCallEntity::getReqBytes).sum();
        return totalScore;
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


    public Long getVisitTotalLine(List<ApiCallEntity> apiCallEntityList) {
        Long totalLine = apiCallEntityList.stream().filter(apiCallEntity -> null != apiCallEntity.getResRows()).mapToLong(ApiCallEntity::getResRows).sum();
        return totalLine;
    }

    /**
     * 处理 where 第一层过滤条件
     *
     * @param where
     */
    private void parseWhereCondition(Where where, Query query) {
        where.forEach((prop, value) -> {
            if (!query.getQueryObject().containsKey(prop)) {
                query.addCriteria(Criteria.where(prop).is(value));
            }
        });
    }

    private void parseFieldCondition(Field fields, Query query) {
        if (null != fields) {
            fields.forEach((filedName, get) -> {
                if ((Boolean) get) {
                    query.fields().include(filedName);
                }
            });
        }
    }

    public List<Map<String, String>> findClients(List<String> moduleIdList) {
        List<Map<String, String>> result = new ArrayList<>();
        Query query = Query.query(Criteria.where("is_deleted").ne(true));
        if (CollectionUtils.isNotEmpty(moduleIdList)) {
            query.addCriteria(Criteria.where("allPathId").in(moduleIdList));
        }
        query.fields().include("user_info.clientId");
        List<ApiCallEntity> modulesDtoList = mongoOperations.find(query, ApiCallEntity.class);
        List<Map> userInfoMap = modulesDtoList.stream().map(ApiCallEntity::getUserInfo).collect(Collectors.toList());

        List<String> clientIdList = new ArrayList<>();
        userInfoMap.forEach(userInfo -> {
            String clientId = (String) userInfo.get("clientId");
            clientIdList.add(clientId);
        });

        List<ApplicationDto> applicationDtoList = applicationService.findByIds(clientIdList);
        applicationDtoList.forEach(applicationDto -> {
            Map<String, String> map = new HashMap<>();
            map.put("id", applicationDto.getId().toString());
            map.put("name", applicationDto.getName());
            result.add(map);
        });

        return result;
    }

    public List<ApiCallEntity> findByClientId(String clientId) {
        Query query = Query.query(Criteria.where("user_info.clientId").is(clientId));
        List<ApiCallEntity> apiCallEntityList = mongoOperations.find(query, ApiCallEntity.class);
        return apiCallEntityList;
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

    /**
     * 处理filter里面的or 请求，传话成Criteria
     *
     * @param where
     * @return
     */
    private Criteria parseOrToCriteria(Where where, String queryField) {
        //处理关键字搜索
        Criteria nameCriteria = new Criteria();
        if (null != where.get("or")) {
            List<Criteria> criteriaList = new ArrayList<>();
            List<Map<String, Map<String, String>>> orList = (List) where.remove("or");
            for (Map<String, Map<String, String>> orMap : orList) {
                orMap.forEach((key, value) -> {
                    if (value.containsKey("$regex")) {
                        String queryStr = value.get("$regex");
                        if (queryField.equals(key)) {
                            Criteria orCriteria = Criteria.where(key).regex(queryStr);
                            criteriaList.add(orCriteria);
                        }
                    }
                });
            }
            nameCriteria = new Criteria().orOperator(criteriaList);
        }
        return nameCriteria;
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

    public List<ApiCallEntity> findMonitorApiCall(ApiDetailParam apiDetailParam) {
        List<ApiCallEntity> apiCallEntityList = new ArrayList<>();

        List<String> clientIdList = apiDetailParam.getClientId();

        List<String> moduleIds = new ArrayList<>();
        moduleIds.add(apiDetailParam.getId());
        Criteria criteria = Criteria.where("allPathId").in(moduleIds);

        criteria.and("user_info.clientId").in(clientIdList);

        Query query = new Query().addCriteria(criteria);
        apiCallEntityList = mongoOperations.find(query, ApiCallEntity.class);

        return apiCallEntityList;
    }


}