package com.tapdata.tm.ds.service.impl;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.util.CapabilityEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.DataSourceDefinitionUpdateDto;
import com.tapdata.tm.ds.dto.DataSourceTypeDto;
import com.tapdata.tm.ds.entity.DataSourceDefinitionEntity;
import com.tapdata.tm.ds.repository.DataSourceDefinitionRepository;
import lombok.NonNull;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Set;

public abstract class DataSourceDefinitionService extends BaseService<DataSourceDefinitionDto, DataSourceDefinitionEntity, ObjectId, DataSourceDefinitionRepository> {
    public DataSourceDefinitionService(@NonNull DataSourceDefinitionRepository repository) {
        super(repository, DataSourceDefinitionDto.class, DataSourceDefinitionEntity.class);
    }
    public abstract DataSourceDefinitionDto update(UserDetail user, DataSourceDefinitionUpdateDto definition);

    public abstract void delete(UserDetail user, String id);

    public abstract Page<DataSourceDefinitionDto> list(UserDetail user, Filter filter);

    public abstract DataSourceDefinitionDto findById(ObjectId id, UserDetail user);

    public abstract DataSourceDefinitionDto findByPdkHash(String pdkHash, Integer pdkBuildNumber, UserDetail user, String... field);

    public abstract List<DataSourceDefinitionDto> findByPdkHashList(Set<String> pdkHashList, UserDetail user);

    public abstract List<DataSourceTypeDto> dataSourceTypes(UserDetail user, Filter filter);

    public abstract DataSourceDefinitionDto getByDataSourceType(String dataSourceType, UserDetail user);

    public abstract List<DataSourceDefinitionDto> getByDataSourceType(List<String> dataSourceType, UserDetail user, String... fields);

    public abstract boolean isAllTypeSchemaFree(List<String> dataSourceType, UserDetail user);

    public abstract boolean checkHasSomeCapability(Set<String> pdkHashList, UserDetail userDetail, CapabilityEnum... eventTypes);

    public abstract DataSourceDefinitionDto convertToDto(DataSourceDefinitionEntity entity, Class dtoClass, String... ignoreProperties);

    public abstract DataSourceDefinitionEntity convertToEntity(Class entityClass, BaseDto dto1, String... ignoreProperties);

    public abstract boolean checkExistsByPdkId(String pdkId, UserDetail user);

    public abstract List<DataSourceTypeDto> dataSourceTypesV2(UserDetail user, Filter filter);

    public abstract DataSourceDefinitionDto getMongoDbByDataSourceType(String dataSourceType, String... fields);
}