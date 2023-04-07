package com.tapdata.tm.metadatadefinition.service;

import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import org.bson.types.ObjectId;

public interface ApiAppService {

    MetadataDefinitionDto save(MetadataDefinitionDto metadataDefinition, UserDetail user);

    Page<MetadataDefinitionDto> find(Filter filter, UserDetail user);

    MetadataDefinitionDto updateById(ObjectId id, MetadataDefinitionDto metadataDefinition, UserDetail user);

    MetadataDefinitionDto findById(ObjectId id, Field field, UserDetail user);

    void delete(ObjectId id, UserDetail user);
}
