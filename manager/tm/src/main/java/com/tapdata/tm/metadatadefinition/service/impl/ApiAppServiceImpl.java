package com.tapdata.tm.metadatadefinition.service.impl;

import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.ApiAppService;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class ApiAppServiceImpl implements ApiAppService {

    @Autowired
    private MetadataDefinitionService metadataDefinitionService;

    @Override
    public MetadataDefinitionDto save(MetadataDefinitionDto metadataDefinition, UserDetail user) {
        List<String> itemType = metadataDefinition.getItemType();
        if (itemType == null) {
            itemType = new ArrayList<>();
            itemType.add(MetadataDefinitionDto.ITEM_TYPE_APP);
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
        //发布的api数量等等

        return null;
    }

    @Override
    public MetadataDefinitionDto updateById(ObjectId id, MetadataDefinitionDto metadataDefinition, UserDetail user) {
        return null;
    }

    @Override
    public MetadataDefinitionDto findById(ObjectId id, Field field, UserDetail user) {
        return null;
    }

    @Override
    public void delete(ObjectId id, UserDetail user) {

    }
}
