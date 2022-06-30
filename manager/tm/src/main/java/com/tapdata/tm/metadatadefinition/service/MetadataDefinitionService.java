package com.tapdata.tm.metadatadefinition.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.inspect.bean.Task;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.entity.MetadataDefinitionEntity;
import com.tapdata.tm.metadatadefinition.param.BatchUpdateParam;
import com.tapdata.tm.metadatadefinition.repository.MetadataDefinitionRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.userLog.constant.Modular;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.poi.ss.formula.functions.T;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        List<Map<String, String>> listTags = batchUpdateParam.getListtags();
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


    public List<MetadataDefinitionDto> findByItemtypeAndValue(MetadataDefinitionDto metadataDefinitionDto,UserDetail userDetail){
        List<String> itemType=metadataDefinitionDto.getItemType();
        String value=metadataDefinitionDto.getValue();

        Query query=Query.query(Criteria.where("item_type").in(itemType).and("value").is(value));
        List<MetadataDefinitionDto> metadataDefinitionDtos=findAll(query);
        return metadataDefinitionDtos;
       /* if (CollectionUtils.isNotEmpty(metadataDefinitionDtos)){
            throw new BizException("Inspect.Name.Exist");
        }*/
    }

    /**
     * value 是唯一索引，是不能重复的
     * @param metadataDefinitionDto
     * @param userDetail
     * @return
     */
    public MetadataDefinitionDto save(MetadataDefinitionDto metadataDefinitionDto,UserDetail userDetail){
        String value=metadataDefinitionDto.getValue();
        MetadataDefinitionDto exsitedOne=findOne(Query.query(Criteria.where("value").is(value)));
        List<String> itemType=metadataDefinitionDto.getItemType();
        if (null!=exsitedOne){
            List itemTypeExisted=  exsitedOne.getItemType();
            itemTypeExisted.addAll(itemType);
            Update update=new Update().set("item_type",itemTypeExisted);
            updateById(exsitedOne.getId(),update,userDetail);
        }
        else {
            super.save(metadataDefinitionDto,userDetail);
        }

        return metadataDefinitionDto;

    }

}