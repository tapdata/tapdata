package com.tapdata.tm.ds.service.impl;

import com.mongodb.ConnectionString;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceEnum;
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
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

public interface DataSourceService extends IBaseService<DataSourceConnectionDto, DataSourceEntity, ObjectId, DataSourceRepository>  {

    DataSourceConnectionDto add(DataSourceConnectionDto connectionDto, UserDetail userDetail);

    DataSourceConnectionDto addWithSpecifiedId(DataSourceConnectionDto connectionDto, UserDetail userDetail);

    DataSourceConnectionDto update(UserDetail user, DataSourceConnectionDto updateDto, boolean changeLast);

    //返回oldName, 表示更换名称
    String updateCheck(UserDetail user, DataSourceConnectionDto updateDto);

    void checkAccessNodeAvailable(String accessNodeType, List<String> accessNodeProcessIdList, UserDetail userDetail);

    Page<DataSourceConnectionDto> list(Filter filter, boolean noSchema, UserDetail userDetail);

    DataSourceConnectionDto getById(ObjectId objectId, com.tapdata.tm.base.dto.Field fields, Boolean noSchema, UserDetail user);

    void buildDefinitionParam(List<DataSourceConnectionDto> items, UserDetail user);

    @Deprecated
    @Transactional
    void updateTag(UserDetail user, UpdateTagsDto updateTagsDto);

    DataSourceConnectionDto delete(UserDetail user, String id);

    DataSourceConnectionDto copy(UserDetail user, String id, String requestURI);

    DataSourceConnectionDto customQuery(ObjectId id, String tableName, Boolean schema, UserDetail user);

    void deleteTags(List<ObjectId> tags, UserDetail user);

    void sendTestConnection(DataSourceConnectionDto connectionDto, boolean updateSchema, Boolean submit, UserDetail user);

    void checkConn(DataSourceConnectionDto connectionDto, UserDetail user);

    long upsertByWhere(Where where, Document update, DataSourceConnectionDto connectionDto, UserDetail user);

    void loadSchema(UserDetail user, List<TapTable> tables, DataSourceConnectionDto oldConnectionDto, String expression, String databaseId, Boolean loadSchemaField);

    void updateAfter(UserDetail user, DataSourceConnectionDto connectionDto, String oldName, Boolean submit);

    void updateAfter(UserDetail user, DataSourceConnectionDto connectionDto, Boolean submit);

    List<String> distinct(String field, UserDetail user);

    List<String> databaseType(UserDetail user);

    ValidateTableVo validateTable(String connectionId, List<String> tableList);

    DataSourceConnectionDto findOne(Filter filter, UserDetail user, Boolean noSchema);

    List<DataSourceConnectionDto> findInfoByConnectionIdList(List<String> connectionIdList);

    List<DataSourceConnectionDto> findInfoByConnectionIdList(List<String> connectionIdList, UserDetail user, String... fields);

    Map<String, DataSourceConnectionDto> batchImport(List<DataSourceConnectionDto> connectionDtos, UserDetail user, boolean cover);

    List<DataSourceConnectionDto> listAll(Filter filter, UserDetail loginUser);

    List<String> findIdByName(String name);

    List<SupportListVo> supportList(UserDetail userDetail);

    List<DataSourceConnectionDto> findAllByIds(List<String> ids);

    void updateConnectionOptions(ObjectId id, ConnectionOptions options, UserDetail user);

    Long countTaskByConnectionId(String connectionId, UserDetail userDetail);

    Long countTaskByConnectionId(String connectionId, String syncType, UserDetail userDetail);

    List<TaskDto> findTaskByConnectionId(String connectionId, int limit, UserDetail userDetail);

    List<TaskDto> findTaskByConnectionId(String connectionId, int limit, String syncType, UserDetail userDetail);

    ConnectionStats stats(UserDetail userDetail);

    void loadPartTables(String connectionId, List<TapTable> tables, UserDetail user);

    void batchEncryptConfig();

    DataSourceConnectionDto importEntity(DataSourceConnectionDto dto, UserDetail userDetail);

    List<TaskDto> findUsingDigginTaskByConnectionId(String connectionId, UserDetail user);

    Long countUsingDigginTaskByConnectionId(String connectionId, UserDetail user);

    DataSourceConnectionDto addConnection(DataSourceConnectionDto connectionDto, UserDetail userDetail);

    DataSourceConnectionDto findByIdByCheck(ObjectId id);

    DataSourceConnectionDto findById(ObjectId id, String... fields);

    @Data
    public static class Part {
        private String _id;
        private long count;
    }
}
