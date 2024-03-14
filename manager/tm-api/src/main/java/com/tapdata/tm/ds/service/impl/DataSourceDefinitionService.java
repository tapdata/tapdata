package com.tapdata.tm.ds.service.impl;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.util.CapabilityEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.DataSourceDefinitionUpdateDto;
import com.tapdata.tm.ds.dto.DataSourceTypeDto;
import com.tapdata.tm.ds.entity.DataSourceDefinitionEntity;
import com.tapdata.tm.ds.repository.DataSourceDefinitionRepository;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Set;

public interface DataSourceDefinitionService extends IBaseService<DataSourceDefinitionDto, DataSourceDefinitionEntity, ObjectId, DataSourceDefinitionRepository> {
    DataSourceDefinitionDto update(UserDetail user, DataSourceDefinitionUpdateDto definition);

    void delete(UserDetail user, String id);

    Page<DataSourceDefinitionDto> list(UserDetail user, Filter filter);

    DataSourceDefinitionDto findById(ObjectId id, UserDetail user);

    DataSourceDefinitionDto findByPdkHash(String pdkHash, Integer pdkBuildNumber, UserDetail user, String... field);

    List<DataSourceDefinitionDto> findByPdkHashList(Set<String> pdkHashList, UserDetail user);

    List<DataSourceTypeDto> dataSourceTypes(UserDetail user, Filter filter);

    DataSourceDefinitionDto getByDataSourceType(String dataSourceType, UserDetail user);

    List<DataSourceDefinitionDto> getByDataSourceType(List<String> dataSourceType, UserDetail user, String... fields);

    boolean isAllTypeSchemaFree(List<String> dataSourceType, UserDetail user);

    boolean checkHasSomeCapability(Set<String> pdkHashList, UserDetail userDetail, CapabilityEnum... eventTypes);

    DataSourceDefinitionDto convertToDto(DataSourceDefinitionEntity entity, Class dtoClass, String... ignoreProperties);

    DataSourceDefinitionEntity convertToEntity(Class entityClass, BaseDto dto1, String... ignoreProperties);

    boolean checkExistsByPdkId(String pdkId, UserDetail user);

    List<DataSourceTypeDto> dataSourceTypesV2(UserDetail user, Filter filter);

    DataSourceDefinitionDto getMongoDbByDataSourceType(String dataSourceType, String... fields);
}