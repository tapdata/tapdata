package com.tapdata.tm.transform.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.MetadataTransformerDto;
import com.tapdata.tm.transform.entity.MetadataTransformerEntity;
import com.tapdata.tm.transform.repository.MetadataTransformerRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;


/**
 * @Author:
 * @Date: 2022/03/04
 * @Description:
 */
@Service
@Slf4j
public class MetadataTransformerService extends BaseService<MetadataTransformerDto, MetadataTransformerEntity, ObjectId, MetadataTransformerRepository> {
    public MetadataTransformerService(@NonNull MetadataTransformerRepository repository) {
        super(repository, MetadataTransformerDto.class, MetadataTransformerEntity.class);
    }

    protected void beforeSave(MetadataTransformerDto metadataTransformer, UserDetail user) {

    }

    public MetadataTransformerDto save(MetadataTransformerDto dto) {
        Assert.notNull(dto, "Dto must not be null!");

        MetadataTransformerEntity entity = convertToEntity(MetadataTransformerEntity.class, dto);

        entity = repository.save(entity);

        dto = convertToDto(entity, dtoClass);

        return dto;
    }

    public void updateVersion(String dataFlowId, String stageId, String version) {
        Criteria criteria = Criteria.where("dataFlowId").is(dataFlowId).and("stageId").is(stageId);

        MetadataTransformerDto dto = new MetadataTransformerDto();
        dto.setDataFlowId(dataFlowId);
        dto.setStageId(stageId);
        dto.setVersion(version);
        upsert(new Query(criteria), dto);
    }
}