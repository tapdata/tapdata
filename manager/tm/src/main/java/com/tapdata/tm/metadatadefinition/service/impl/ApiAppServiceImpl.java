package com.tapdata.tm.metadatadefinition.service.impl;

import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatadefinition.dto.ApiAppDetail;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.ApiAppService;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.modules.constant.ModuleStatusEnum;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ApiAppServiceImpl implements ApiAppService {

    @Autowired
    private MetadataDefinitionService metadataDefinitionService;



    @Autowired
    private ModulesService modulesService;

    @Override
    public MetadataDefinitionDto save(MetadataDefinitionDto metadataDefinition, UserDetail user) {
        List<String> itemType = metadataDefinition.getItemType();
        if (itemType == null) {
            itemType = new ArrayList<>();
            itemType.add(MetadataDefinitionDto.ITEM_TYPE_APP);
            metadataDefinition.setItemType(itemType);
        } else {
            if (!itemType.contains(MetadataDefinitionDto.ITEM_TYPE_APP)) {
                itemType.add(MetadataDefinitionDto.ITEM_TYPE_APP);
            }
        }
        return metadataDefinitionService.save(metadataDefinition, user);
    }

    @Override
    public Page<MetadataDefinitionDto> find(Filter filter, UserDetail user) {
        Page<MetadataDefinitionDto> page = metadataDefinitionService.find(filter, user);
        //添加api总数，跟已发布的api数量。
        addApiCount(page.getItems(), user);

        return page;
    }

    @Override
    public MetadataDefinitionDto updateById(ObjectId id, MetadataDefinitionDto metadataDefinition, UserDetail user) {
        MetadataDefinitionDto metadataDefinitionDto = metadataDefinitionService.save(metadataDefinition, user);
        addApiCount(Lists.of(metadataDefinitionDto), user);
        return metadataDefinitionDto;
    }

    @Override
    public MetadataDefinitionDto findById(ObjectId id, Field field, UserDetail user) {
        MetadataDefinitionDto metadataDefinitionDto = metadataDefinitionService.findById(id, field, user);
        addApiCount(Lists.of(metadataDefinitionDto), user);
        return metadataDefinitionDto;
    }

    @Override
    public void move(String oldId, String newId, UserDetail user) {

        Criteria criteriaNew = Criteria.where("_id").is(MongoUtils.toObjectId(newId));
        MetadataDefinitionDto newTag = metadataDefinitionService.findOne(new Query(criteriaNew), user);
        if (newTag == null) {
            throw new BizException("");
        }

        Criteria criteria = Criteria.where("listtags")
                .elemMatch(Criteria.where("id").is(oldId));
        Update update = Update.update("listtags.$.id", newTag.getId()).set("listtags.$.value", newTag.getValue());
        modulesService.update(new Query(criteria), update, user);
    }

    @Override
    public ApiAppDetail detail(ObjectId toObjectId, UserDetail user) {
        MetadataDefinitionDto tag = metadataDefinitionService.findById(toObjectId, user);
        Criteria criteria = Criteria.where("listtags.id").is(toObjectId.toHexString());
        Query query = new Query(criteria);
        List<ModulesDto> modulesDtos = modulesService.findAllDto(query, user);
        if (CollectionUtils.isNotEmpty(modulesDtos)) {
            tag.setApiCount(modulesDtos.size());
            int publishedApiCount = (int) modulesDtos.stream().map(s -> ModuleStatusEnum.ACTIVE.getValue().equals(s.getStatus())).count();
            tag.setPublishedApiCount(publishedApiCount);
        }

        ApiAppDetail apiAppDetail = new ApiAppDetail();
        BeanUtils.copyProperties(tag, apiAppDetail);
        apiAppDetail.setApis(modulesDtos);
        return apiAppDetail;
    }

    private void addApiCount(List<MetadataDefinitionDto> metadatas, UserDetail user) {
        if (CollectionUtils.isEmpty(metadatas)) {
            return;
        }

        List<String> tagIds = metadatas.stream().map(m -> m.getId().toHexString()).collect(Collectors.toList());
        Criteria criteria = Criteria.where("listtags.id").in(tagIds);

        Query query = new Query(criteria);
        query.fields().include("listtags", "status");
        List<ModulesDto> modulesDtos = modulesService.findAllDto(query, user);

        Map<String, List<ModulesDto>> map = new HashMap<>();
        for (ModulesDto modulesDto : modulesDtos) {
            List<Tag> listtags = modulesDto.getListtags();
            for (Tag tag : listtags) {
                if (tagIds.contains(tag.getId())) {
                    List<ModulesDto> list = map.computeIfAbsent(tag.getId(), k -> new ArrayList<>());
                    list.add(modulesDto);
                    break;
                }
            }
        }

        for (MetadataDefinitionDto metadata : metadatas) {
            List<ModulesDto> modulesDtos1 = map.get(metadata.getId().toHexString());
            boolean empty = CollectionUtils.isEmpty(modulesDtos1);
            int apiCount = empty ? 0 : modulesDtos1.size();
            int publishedApiCount = empty ? 0 : (int) modulesDtos1.stream().map(s -> ModuleStatusEnum.ACTIVE.getValue().equals(s.getStatus())).count();
            metadata.setApiCount(apiCount);
            metadata.setPublishedApiCount(publishedApiCount);
        }

    }
}
