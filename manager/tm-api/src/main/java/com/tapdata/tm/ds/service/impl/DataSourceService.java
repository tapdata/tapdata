package com.tapdata.tm.ds.service.impl;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.ConnectionStats;
import com.tapdata.tm.ds.dto.UpdateTagsDto;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.ds.vo.SupportListVo;
import com.tapdata.tm.ds.vo.ValidateTableVo;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import lombok.Data;
import lombok.NonNull;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

public abstract class DataSourceService extends BaseService<DataSourceConnectionDto, DataSourceEntity, ObjectId, DataSourceRepository> {
    public DataSourceService(@NonNull DataSourceRepository repository) {
        super(repository, DataSourceConnectionDto.class, DataSourceEntity.class);
    }

    public abstract DataSourceConnectionDto add(DataSourceConnectionDto connectionDto, UserDetail userDetail);

    public abstract DataSourceConnectionDto addWithSpecifiedId(DataSourceConnectionDto connectionDto, UserDetail userDetail);

    public abstract DataSourceConnectionDto update(UserDetail user, DataSourceConnectionDto updateDto, boolean changeLast);

    //返回oldName, 表示更换名称
    public abstract String updateCheck(UserDetail user, DataSourceConnectionDto updateDto);

    public abstract void checkAccessNodeAvailable(String accessNodeType, List<String> accessNodeProcessIdList, UserDetail userDetail);

    public abstract Page<DataSourceConnectionDto> list(Filter filter, boolean noSchema, UserDetail userDetail);

    public abstract DataSourceConnectionDto getById(ObjectId objectId, com.tapdata.tm.base.dto.Field fields, Boolean noSchema, UserDetail user);

    public abstract void buildDefinitionParam(List<DataSourceConnectionDto> items, UserDetail user);

    @Deprecated
    @Transactional
    public abstract void updateTag(UserDetail user, UpdateTagsDto updateTagsDto);

    public abstract DataSourceConnectionDto delete(UserDetail user, String id);

    public abstract DataSourceConnectionDto copy(UserDetail user, String id, String requestURI);

    public abstract DataSourceConnectionDto customQuery(ObjectId id, String tableName, Boolean schema, UserDetail user);

    public abstract void deleteTags(List<ObjectId> tags, UserDetail user);

    public abstract void sendTestConnection(DataSourceConnectionDto connectionDto, boolean updateSchema, Boolean submit, UserDetail user);

    public abstract void checkConn(DataSourceConnectionDto connectionDto, UserDetail user);

    public abstract long upsertByWhere(Where where, Document update, DataSourceConnectionDto connectionDto, UserDetail user);

    public abstract void loadSchema(UserDetail user, List<TapTable> tables, DataSourceConnectionDto oldConnectionDto, String expression, String databaseId, Boolean loadSchemaField,Boolean partLoad);

    public abstract void updateAfter(UserDetail user, DataSourceConnectionDto connectionDto, String oldName, Boolean submit);

    public abstract void updateAfter(UserDetail user, DataSourceConnectionDto connectionDto, Boolean submit);

    public abstract List<String> distinct(String field, UserDetail user);

    public abstract List<String> databaseType(UserDetail user);

    public abstract ValidateTableVo validateTable(String connectionId, List<String> tableList);

    public abstract DataSourceConnectionDto findOne(Filter filter, UserDetail user, Boolean noSchema);

    public abstract List<DataSourceConnectionDto> findInfoByConnectionIdList(List<String> connectionIdList);

    public abstract List<DataSourceConnectionDto> findInfoByConnectionIdList(List<String> connectionIdList, UserDetail user, String... fields);

    public abstract Map<String, DataSourceConnectionDto> batchImport(List<DataSourceConnectionDto> connectionDtos, UserDetail user, boolean cover);

    public abstract List<DataSourceConnectionDto> listAll(Filter filter, UserDetail loginUser);

    public abstract List<String> findIdByName(String name);

    public abstract List<SupportListVo> supportList(UserDetail userDetail);

    public abstract List<DataSourceConnectionDto> findAllByIds(List<String> ids);

    public abstract void updateConnectionOptions(ObjectId id, ConnectionOptions options, UserDetail user);

    public abstract Long countTaskByConnectionId(String connectionId, UserDetail userDetail);

    public abstract Long countTaskByConnectionId(String connectionId, String syncType, UserDetail userDetail);

    public abstract List<TaskDto> findTaskByConnectionId(String connectionId, int limit, UserDetail userDetail);

    public abstract List<TaskDto> findTaskByConnectionId(String connectionId, int limit, String syncType, UserDetail userDetail);

    public abstract ConnectionStats stats(UserDetail userDetail);

    public abstract void loadPartTables(String connectionId, List<TapTable> tables, UserDetail user);

    public abstract void batchEncryptConfig();

    public abstract DataSourceConnectionDto importEntity(DataSourceConnectionDto dto, UserDetail userDetail);

    public abstract List<TaskDto> findUsingDigginTaskByConnectionId(String connectionId, UserDetail user);

    public abstract Long countUsingDigginTaskByConnectionId(String connectionId, UserDetail user);

    public abstract DataSourceConnectionDto addConnection(DataSourceConnectionDto connectionDto, UserDetail userDetail);

    public abstract DataSourceConnectionDto findByIdByCheck(ObjectId id);

    public abstract DataSourceConnectionDto findById(ObjectId id, String... fields);

    public abstract void flushDatabaseMetadataInstanceLastUpdate(String loadFieldsStatus,String connectionId,Long lastUpdate,UserDetail user);

    @Data
    public static class Part {
        private String _id;
        private long count;
    }
}
