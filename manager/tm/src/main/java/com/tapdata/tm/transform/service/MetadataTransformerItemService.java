package com.tapdata.tm.transform.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.MetadataTransformerItemDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.transform.entity.MetadataTransformerItemEntity;
import com.tapdata.tm.transform.repository.MetadataTransformerItemRepository;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author:
 * @Date: 2022/03/04
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class MetadataTransformerItemService extends BaseService<MetadataTransformerItemDto, MetadataTransformerItemEntity, ObjectId, MetadataTransformerItemRepository> {

    private TaskService taskService;

    public MetadataTransformerItemService(@NonNull MetadataTransformerItemRepository repository) {
        super(repository, MetadataTransformerItemDto.class, MetadataTransformerItemEntity.class);
    }

    protected void beforeSave(MetadataTransformerItemDto metadataTransformerItem, UserDetail user) {

    }


    public  List<MetadataTransformerItemDto> save(List<MetadataTransformerItemDto> dtoList) {
        Assert.notNull(dtoList, "Dto must not be null!");

        List<MetadataTransformerItemEntity> entityList = new ArrayList<>();
        for (MetadataTransformerItemDto dto : dtoList) {
            MetadataTransformerItemEntity entity = convertToEntity(MetadataTransformerItemEntity.class, dto);
            entityList.add(entity);
        }

        entityList = repository.saveAll(entityList);

        dtoList = convertToDto(entityList, dtoClass);

        return dtoList;
    }

    public Update buildUpdateSet(MetadataTransformerItemDto transformerItemDto) {
        MetadataTransformerItemEntity entity = convertToEntity(MetadataTransformerItemEntity.class, transformerItemDto);
        return repository.buildUpdateSet(entity);
    }

    public Page<MetadataTransformerItemDto> findByFilter(Filter filter) {
        return find(filter);
    }

    public void bulkUpsert(List<MetadataTransformerItemDto> metadataTransformerItemDtos) {

        //更新两张中间状态表
        BulkOperations bulkOperations = repository.getMongoOperations().bulkOps(BulkOperations.BulkMode.UNORDERED, MetadataTransformerItemEntity.class);
        for (MetadataTransformerItemDto itemDto : metadataTransformerItemDtos) {

            Criteria criteria = Criteria.where("dataFlowId").is(itemDto.getDataFlowId())
                    .and("sinkNodeId").is(itemDto.getSinkNodeId())
                    .and("sinkTableId").is(itemDto.getSinkTableId());

            Query query = new Query(criteria);
            Update update = buildUpdateSet(itemDto);
            bulkOperations.upsert(query, update);
        }

        bulkOperations.execute();
    }
}