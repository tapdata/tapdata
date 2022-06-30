package com.tapdata.tm.ds.service.impl;

import cn.hutool.extra.cglib.CglibUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.util.CapabilityEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.ds.dto.DataSourceDefinitionUpdateDto;
import com.tapdata.tm.ds.dto.DataSourceTypeDto;
import com.tapdata.tm.ds.entity.DataSourceDefinitionEntity;
import com.tapdata.tm.ds.repository.DataSourceDefinitionRepository;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MessageUtil;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.MongoUtils.*;

/**
 * @Author: Zed
 * @Date: 2021/8/20
 * @Description:
 */
@Service
@Slf4j
public class DataSourceDefinitionService extends BaseService<DataSourceDefinitionDto, DataSourceDefinitionEntity, ObjectId, DataSourceDefinitionRepository> {
    public DataSourceDefinitionService(@NonNull DataSourceDefinitionRepository repository) {
        super(repository, DataSourceDefinitionDto.class, DataSourceDefinitionEntity.class);
    }

    /**
     * 修改数据源定义信息
     *
     * @param user
     * @param definition 修改后的名称
     * @return
     */

    public DataSourceDefinitionDto update(UserDetail user, DataSourceDefinitionUpdateDto definition) {
        //校验用户是否拥有修改数据源定义的权限
        //根据数据源定义id查询数据源定义信息，
        if (definition == null) {
            log.warn("Data source type is not found");
            throw  new BizException("DatasourceDefine.NotFound", "Data source type is not found");
        }

        DataSourceDefinitionDto dataSourceDefinitionDto = findById(definition.getId(), user);
        if (dataSourceDefinitionDto == null) {
            log.warn("data source definition not found");
            throw new BizException("DatasourceDefine.NotFound", "data source definition not found");
        }


        DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
        BeanUtils.copyProperties(definition, definitionDto);

        return save(definitionDto, user);
    }

    /**
     * 根据id删除数据源定义
     *
     * @param user
     * @param id
     * @return
     */

    public void delete(UserDetail user, String id) {
        //检验当前用户是否用户删除数据源定义的权限
        //根据id删除数据源定义
        Optional<DataSourceDefinitionEntity> optional = repository.findById(toObjectId(id), user);
        if (!optional.isPresent()) {
            throw new BizException("DatasourceDefine.NotFound", "data source definition not found or not belong to current user");
        }
        deleteById(new ObjectId(id));
    }

    /**
     * 查询数据源类型信息列表
     * @param user
     * @param filter
     * @return
     */
    public Page<DataSourceDefinitionDto> list(UserDetail user, Filter filter) {
        //根据名称过滤与分页信息，查询数据源定义列表并返还
        //如果是官方的，则所有人可以查询，否则只能查询用户自己添加的类型信息
        Query query = repository.filterToSimpleQuery(filter);
        Document queryObject = query.getQueryObject();
        Criteria userCriteria = new Criteria();
        if (!queryObject.containsKey("customId")) {
            userCriteria.and("customId").is(user.getCustomerId());
        }

        if (!user.isRoot()) {
            if (!queryObject.containsKey("user_id")) {
                userCriteria.and("user_id").is(user.getUserId());
            }
        }
        Criteria supplierCriteria = Criteria.where("supplierType").is("self");
        Criteria criteria = repository.filterBuildCriteria(filter);
        criteria.orOperator(userCriteria, supplierCriteria);
        query.addCriteria(criteria);
        List<DataSourceDefinitionEntity> definitionEntities = repository.findAll(query);
        query.limit(0);
        query.skip(0);
        long count = repository.count(query);
        Page<DataSourceDefinitionDto> page = new Page<>();
        page.setItems(convertToDto(definitionEntities, DataSourceDefinitionDto.class));
        page.setTotal(count);
        return page;
    }


    /**
     * find model by id
     * @param id
     * @param user
     * @return
     */
    public DataSourceDefinitionDto findById(ObjectId id, UserDetail user) {
        Criteria userCriteria = new Criteria();
        userCriteria.and("customId").is(user.getCustomerId());
        Criteria supplierCriteria = Criteria.where("supplierType").ne("self");
        Criteria criteria = Criteria.where("_id").is(id);
        criteria.orOperator(userCriteria, supplierCriteria);
        Optional<DataSourceDefinitionEntity> optional = repository.findOne(Query.query(criteria));
        return optional.map(dataSourceDefinitionEntity -> convertToDto(dataSourceDefinitionEntity, DataSourceDefinitionDto.class)).orElse(null);
    }

    /**
     * find model by pdk hash
     * @param pdkHash
     * @param user
     * @return
     */
    public DataSourceDefinitionDto findByPdkHash(String pdkHash, UserDetail user) {
        Criteria userCriteria = new Criteria();
        userCriteria.and("customId").is(user.getCustomerId());
        Criteria supplierCriteria = Criteria.where("supplierType").ne("self");
        Criteria criteria = Criteria.where("pdkHash").is(pdkHash);
        criteria.orOperator(userCriteria, supplierCriteria);
        Optional<DataSourceDefinitionEntity> optional = repository.findOne(Query.query(criteria));
        DataSourceDefinitionDto dataSourceDefinition = optional.map(dataSourceDefinitionEntity -> convertToDto(dataSourceDefinitionEntity, DataSourceDefinitionDto.class)).orElse(null);

        try {
            updateConfigPropertiesTitle(dataSourceDefinition);
        } catch (Exception e) {
            log.error("findByPdkHash updateConfigPropertiesTitle", e);
        }

        return dataSourceDefinition;
    }

    public List<DataSourceDefinitionDto> findByPdkHashList(List<String> pdkHashList, UserDetail user) {
        Criteria userCriteria = new Criteria();
        userCriteria.and("customId").is(user.getCustomerId());
        Criteria supplierCriteria = Criteria.where("supplierType").ne("self");
        Criteria criteria = Criteria.where("pdkHash").in(pdkHashList);
        criteria.orOperator(userCriteria, supplierCriteria);

        return findAll(Query.query(criteria));
    }

    /**
     * 按照cookie的lang更新属性title，支持pdk多语言
     * @param dataSourceDefinition dataSourceDefinition
     */
    private void updateConfigPropertiesTitle(DataSourceDefinitionDto dataSourceDefinition) {
        if (!Objects.isNull(dataSourceDefinition)
                && !Objects.isNull(dataSourceDefinition.getProperties())) {
            LinkedHashMap<String, Object> properties = dataSourceDefinition.getProperties();
            final String[] content = {JSON.toJSONString(properties)};

            LinkedHashMap<String, Object> messages = dataSourceDefinition.getMessages();
            String language = MessageUtil.getLanguage();
            Object o = messages.get(language);
            LinkedHashMap<String, Object> msgJson = JSON.parseObject(JSON.toJSONString(o), new TypeReference<LinkedHashMap<String, Object>>(){});

            msgJson.forEach((key, value) -> content[0] = content[0].replaceAll(new StringJoiner(key).add("\\$\\{").add("}").toString(), value.toString()));
            LinkedHashMap<String, Object> temp = JSON.parseObject(content[0], new TypeReference<LinkedHashMap<String, Object>>(){});

            dataSourceDefinition.setProperties(temp);
        }
    }

    /**
     * 查询数据源类型信息列表
     * @param user
     * @param filter
     * @return
     */
    public List<DataSourceTypeDto> dataSourceTypes(UserDetail user, Filter filter) {
        //根据名称过滤与分页信息，查询数据源定义列表并返还
        //页面查询列表只需要查询数据源定义类型信息，不需要配置信息，所以当filter中查询字段为空时，只查询非配置的信息
        if (filter.getFields() == null || filter.getFields().size() == 0) {
            Field field = new Field();
            field.put("properties", false);
            filter.setFields(field);
        }
        //如果是官方的，则所有人可以查询，否则只能查询用户自己添加的类型信息
//        Query query = repository.filterToSimpleQuery(filter);

//        Document queryObject = query.getQueryObject();
        Where where= filter.getWhere();
        Query query=new Query();
        Criteria userCriteria = new Criteria();
        if (!where.containsKey("customId")) {
            userCriteria.and("customId").is(user.getCustomerId());
        }

        if (!user.isRoot()) {
            if (!where.containsKey("user_id")) {
                userCriteria.and("user_id").is(user.getUserId());
            }
        }
        //Criteria supplierCriteria = Criteria.where("supplierType").ne("self");
        Criteria supplierCriteria = Criteria.where("pdkType").ne(DataSourceDefinitionDto.PDK_TYPE);
        Criteria criteria = repository.filterBuildCriteria(filter);
        criteria.orOperator(userCriteria, supplierCriteria, Criteria.where("scope").is("public"));
        // only return the latest version
        criteria.and("latest").is(true);
        query.addCriteria(criteria);
        query.limit(0);
        query.skip(0);
        List<DataSourceDefinitionEntity> definitionEntities = repository.findAll(query);

        long count = repository.count(query);

        // distinct
        definitionEntities = definitionEntities.stream().collect(
                Collectors.collectingAndThen(
                        Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(DataSourceDefinitionEntity::getPdkId))), ArrayList::new
                )
        );

        List<DataSourceTypeDto> typeList = CglibUtil.copyList(definitionEntities, DataSourceTypeDto::new);

        Page<DataSourceTypeDto> dataSourceTypeDtoPage = new Page<>();
        dataSourceTypeDtoPage.setTotal(count);
        dataSourceTypeDtoPage.setItems(typeList);
        return dataSourceTypeDtoPage.getItems();
    }


    public DataSourceDefinitionDto getByDataSourceType(String dataSourceType, UserDetail user) {
        List<DataSourceDefinitionDto> definitionDtos = getByDataSourceType(Lists.of(dataSourceType), user);
        if (CollectionUtils.isNotEmpty(definitionDtos)) {
            return definitionDtos.get(0);
        }
        return null;
    }
    public List<DataSourceDefinitionDto> getByDataSourceType(List<String> dataSourceType, UserDetail user) {
        Criteria customCriteria = new Criteria();
        customCriteria.and("customId").is(user.getCustomerId());
        Criteria userCriteria = Criteria.where("user_id").is(user.getUserId());
        Criteria supplierCriteria = Criteria.where("pdkType").ne(DataSourceDefinitionDto.PDK_TYPE);
        Criteria scopeCriteria = Criteria.where("scope").is("public");
        Criteria criteria = Criteria.where("type").in(dataSourceType);
        criteria.orOperator(customCriteria, userCriteria, supplierCriteria, scopeCriteria);
        return findAllDto(Query.query(criteria), user);
    }


    protected void beforeSave(DataSourceDefinitionDto dto, UserDetail userDetail) {
        dto.setId(null);
    }

    public boolean checkHasSomeCapability(List<String> pdkHashList, UserDetail userDetail, CapabilityEnum eventType) {
        List<DataSourceDefinitionDto> definitionList = findByPdkHashList(pdkHashList, userDetail);
        if (CollectionUtils.isNotEmpty(definitionList)) {
            Optional<List<String>> optional = definitionList.stream()
                    .map(DataSourceDefinitionDto::getCapabilities)
                    .filter(cap -> Objects.isNull(cap) || CollectionUtils.isEmpty(cap))
                    .findFirst();
            if (optional.isPresent()) {
                return false;
            }

            List<List<String>> capabilities = definitionList.stream()
                    .map(DataSourceDefinitionDto::getCapabilities)
                    .filter(Objects::nonNull).collect(Collectors.toList());

            Optional<List<String>> reduce = capabilities.stream().reduce((a, b) -> {
                a.retainAll(b);
                return a;
            });

            if (reduce.isPresent()) {
                List<String> intersection = reduce.orElse(new ArrayList<>());
                if (intersection.contains(eventType.getEvent())) {
                    return true;
                }
            }

        }
        return false;
    }
}
