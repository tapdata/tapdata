package com.tapdata.tm.metadatadefinition.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.discovery.service.DiscoveryService;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.entity.MetadataDefinitionEntity;
import com.tapdata.tm.metadatadefinition.param.BatchUpdateParam;
import com.tapdata.tm.metadatadefinition.repository.MetadataDefinitionRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Service
@Slf4j
public class MetadataDefinitionService extends BaseService<MetadataDefinitionDto, MetadataDefinitionEntity, ObjectId, MetadataDefinitionRepository> {

    @Autowired
    MongoTemplate mongoTemplate;


    @Autowired
    MetadataInstancesService metadataInstancesService;

    @Autowired
    private DiscoveryService discoveryService;

    @Autowired
    private DataSourceDefinitionService definitionService;

    @Autowired
    private UserService userService;

    public MetadataDefinitionService(@NonNull MetadataDefinitionRepository repository) {
        super(repository, MetadataDefinitionDto.class, MetadataDefinitionEntity.class);
    }

    protected void beforeSave(MetadataDefinitionDto metadataDefinition, UserDetail user) {

    }

    /**
     * 批量修改分类, 每条数据只能有一个分类
     *
     * @param tableName tableName
     * @param batchUpdateParam batchUpdateParam
     */
    public List<String> batchUpdateListTags(String tableName, BatchUpdateParam batchUpdateParam,UserDetail userDetail) {
        List<String> idList = batchUpdateParam.getId();
        List<Tag> listTags = batchUpdateParam.getListtags();
        //todo updateMulti  如果用表名传入，更新不了多条，只能用for循环更新，应该优化成直接更新多条
        //todo 改成动态实例来更新数据
        Update update=new Update().set("listtags",listTags);
        if ("Connections".equals(tableName)){
            UpdateResult updateResult = mongoTemplate.updateMulti(Query.query(Criteria.where("id").in(idList)), update, DataSourceEntity.class);
        }
        else if ("Task".equals(tableName)) {
            UpdateResult updateResult = mongoTemplate.updateMulti(Query.query(Criteria.where("id").in(idList)), update, TaskEntity.class);
        }
        else if ("Modules".equals(tableName)) {
            UpdateResult updateResult = mongoTemplate.updateMulti(Query.query(Criteria.where("id").in(idList)), update, ModulesEntity.class);
        }
        //更新成功后，需要将模型中的也跟着更新了
        Criteria criteria = Criteria.where("source.id").in(idList).and("metaType").is("database").and("isDeleted").is(false);
        Update classifications = Update.update("classifications", listTags);
        metadataInstancesService.update(new Query(criteria), classifications, userDetail);
        return null;
    }


    public void findByItemtypeAndValue(MetadataDefinitionDto metadataDefinitionDto,UserDetail userDetail){
        String value=metadataDefinitionDto.getValue();


        String parentId = metadataDefinitionDto.getParent_id();
        Criteria criteria = Criteria.where("value").is(value);
        if (StringUtils.isBlank(parentId)) {
            criteria.and("parent_id").exists(false).and("item_type").in(metadataDefinitionDto.getItemType());
        } else {
            criteria.and("parent_id").is(parentId);
        }

        Query query=Query.query(criteria);
        query.fields().exclude("_id");
        List<MetadataDefinitionDto> metadataDefinitionDtos =findAll(query);
        if (CollectionUtils.isNotEmpty(metadataDefinitionDtos)){
            throw new BizException("Tag.RepeatName");
        }
    }

    /**
     * value 是唯一索引，是不能重复的
     * @param metadataDefinitionDto
     * @param userDetail
     * @return
     */
    public MetadataDefinitionDto save(MetadataDefinitionDto metadataDefinitionDto,UserDetail userDetail){
        MetadataDefinitionDto exsitedOne = null;
        if (metadataDefinitionDto.getId() != null) {
            exsitedOne = findById(metadataDefinitionDto.getId());
        }
        List<String> itemType=metadataDefinitionDto.getItemType();
        if (null!=exsitedOne){
            List<String> itemTypeExisted=  exsitedOne.getItemType();
            if (itemTypeExisted == null) {
                itemTypeExisted = new ArrayList<>();
            }
            metadataDefinitionDto.setItemType(itemTypeExisted);
            if (CollectionUtils.isNotEmpty(itemType)) {
                for (String s : itemType) {
                    if (!itemTypeExisted.contains(s)) {
                        itemTypeExisted.add(s);
                    }
                }
            }
        }

        MetadataDefinitionDto saveValue = super.save(metadataDefinitionDto, userDetail);

        if (exsitedOne != null) {
            if (CollectionUtils.isNotEmpty(saveValue.getItemType()) && StringUtils.isNotBlank(metadataDefinitionDto.getValue())
                    && !metadataDefinitionDto.getValue().equals(exsitedOne.getValue())) {
                Criteria criteria = Criteria.where("listtags")
                        .elemMatch(Criteria.where("id").is(metadataDefinitionDto.getId().toHexString()));
                Update update = Update.update("listtags.$.value", metadataDefinitionDto.getValue());
                if (saveValue.getItemType().contains("dataflow")){
                    mongoTemplate.updateMulti(new Query(criteria), update, TaskEntity.class);
                }

                if (saveValue.getItemType().contains("database")){
                    mongoTemplate.updateMulti(new Query(criteria), update, DataSourceEntity.class);
                }
            }

        }

        return saveValue;


    }


    public List<MetadataDefinitionDto> findAndParent(List<MetadataDefinitionDto> metadataDefinitionDtos, List<ObjectId> idList) {
        Criteria criteria = Criteria.where("_id").in(idList);
        Query query = new Query(criteria);
        List<MetadataDefinitionDto> all = findAll(query);
        if (metadataDefinitionDtos == null) {
            metadataDefinitionDtos = new ArrayList<>();
        }
        metadataDefinitionDtos.addAll(all);
        List<ObjectId> ids = all.stream().filter(a -> StringUtils.isNotBlank(a.getParent_id())).map(a -> MongoUtils.toObjectId(a.getParent_id())).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(ids)) {
            return metadataDefinitionDtos;
        }

        return findAndParent(metadataDefinitionDtos, ids);
    }


    public List<MetadataDefinitionDto> findAndChild(List<ObjectId> idList) {
        Criteria criteria = Criteria.where("_id").in(idList);
        Query query = new Query(criteria);

        List<MetadataDefinitionDto> all = findAll(query);
        return findChild(all, idList);
    }

    public List<MetadataDefinitionDto> findAndChild(List<MetadataDefinitionDto> all, MetadataDefinitionDto dto, Map<String, List<MetadataDefinitionDto>> parentMap) {
        if (all == null) {
            all = new ArrayList<>();
            all.add(dto);
        }

        List<MetadataDefinitionDto> metadataDefinitionDtos = parentMap.get(dto.getId().toHexString());
        if (CollectionUtils.isNotEmpty(metadataDefinitionDtos)) {
            all.addAll(metadataDefinitionDtos);
            for (MetadataDefinitionDto metadataDefinitionDto : metadataDefinitionDtos) {
                findAndChild(all, metadataDefinitionDto, parentMap);
            }
        }
        return all;
    }


    private List<MetadataDefinitionDto> findChild(List<MetadataDefinitionDto> metadataDefinitionDtos, List<ObjectId> idList) {
        List<String> collect = idList.stream().map(ObjectId::toHexString).collect(Collectors.toList());
        Criteria criteria = Criteria.where("parent_id").in(collect);
        Query query = new Query(criteria);
        List<MetadataDefinitionDto> all = findAll(query);
        if (CollectionUtils.isEmpty(all)) {
            return metadataDefinitionDtos;
        }
        metadataDefinitionDtos.addAll(all);
        List<ObjectId> ids = all.stream().map(BaseDto::getId).collect(Collectors.toList());
        return findChild(metadataDefinitionDtos, ids);
    }

    @Override
    public Page<MetadataDefinitionDto> find(Filter filter, UserDetail user) {
        Page<MetadataDefinitionDto> dtoPage = super.find(filter, user);
        dtoPage.getItems().sort(Comparator.comparing(MetadataDefinitionDto::getValue));
        dtoPage.getItems().sort(Comparator.comparing(s -> {
            List<String> itemType = s.getItemType();
            return !itemType.contains("default");
        }));
        Field fields = filter.getFields();
        if (fields != null) {
            Object objCount = fields.get("objCount");
            if (objCount != null && (objCount.equals(true) || (Double) objCount == 1) && CollectionUtils.isNotEmpty(dtoPage.getItems())) {
                discoveryService.addObjCount(dtoPage.getItems(), user);

                if (CollectionUtils.isNotEmpty(dtoPage.getItems())) {
                    List<MetadataDefinitionDto> delItems = new ArrayList<>();
                    List<ObjectId> pdkIdDirectories = new ArrayList<>();
                    Criteria criteriaDefinition = Criteria.where("pdkType").is("pdk")
                            .and("is_deleted").ne(true);
                    Query queryDefinition = new Query(criteriaDefinition);
                    queryDefinition.fields().include("pdkId");
                    List<DataSourceDefinitionDto> dataSourceDefinitionDtos = definitionService.findAllDto(queryDefinition, user);
                    if (CollectionUtils.isNotEmpty(dataSourceDefinitionDtos)) {
                        List<String> pdkIds = dataSourceDefinitionDtos.stream().map(DataSourceDefinitionDto::getPdkId).distinct().collect(Collectors.toList());
                        Criteria in = Criteria.where("item_type").is("default").and("value").in(pdkIds);
                        Query query1 = new Query(in);
                        query1.fields().include("_id");
                        List<MetadataDefinitionDto> pdkDirectories = findAllDto(query1, user);
                        if (CollectionUtils.isNotEmpty(pdkDirectories)) {
                            pdkIdDirectories = pdkDirectories.stream().map(BaseDto::getId).collect(Collectors.toList());
                        }
                    }
                    for (MetadataDefinitionDto item : dtoPage.getItems()) {
                        if ((StringUtils.isNotBlank(item.getLinkId()) || pdkIdDirectories.contains(item.getId())) && item.getObjCount() < 1) {
                            delItems.add(item);
                        }
                    }
                    dtoPage.getItems().removeAll(delItems);
                }
            }
        }

        List<String> userIdList = dtoPage.getItems().stream()
                .filter(d -> d.getItemType().contains("root"))
                .map(BaseDto::getUserId)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(userIdList)) {
            Map<String, UserDetail> userMap = userService.getUserMapByIdList(userIdList);
            for (MetadataDefinitionDto item : dtoPage.getItems()) {
                if (item.getItemType().contains("root")) {
                    UserDetail userDetail = userMap.get(item.getUserId());
                    if (userDetail != null) {
                        item.setUserName(StringUtils.isBlank(userDetail.getUsername()) ? userDetail.getEmail() : userDetail.getUsername());
                    }
                }
            }
        }
        return dtoPage;
    }
}