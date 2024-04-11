package com.tapdata.tm.metadatainstance.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.Table;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.bean.DiscoveryFieldDto;
import com.tapdata.tm.metadatainstance.bean.MultiPleTransformReq;
import com.tapdata.tm.metadatainstance.dto.DataType2TapTypeDto;
import com.tapdata.tm.metadatainstance.dto.DataTypeCheckMultipleVo;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.param.ClassificationParam;
import com.tapdata.tm.metadatainstance.param.TablesSupportInspectParam;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.metadatainstance.vo.*;
import com.tapdata.tm.utils.MetadataUtil;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Update;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class MetadataInstancesService extends BaseService<MetadataInstancesDto, MetadataInstancesEntity, ObjectId, MetadataInstancesRepository> {
    public MetadataInstancesService(@NonNull MetadataInstancesRepository repository) {
        super(repository, MetadataInstancesDto.class, MetadataInstancesEntity.class);
    }
    public abstract MetadataInstancesDto add(MetadataInstancesDto record, UserDetail user);

    public abstract MetadataInstancesDto modifyById(ObjectId id, MetadataInstancesDto record, UserDetail user);

    public abstract Page<MetadataInstancesDto> list(Filter filter, UserDetail user);

    public abstract List<MetadataInstancesVo> findInspect(Filter filter, UserDetail userDetail);

    public abstract MetadataInstancesDto queryById(ObjectId id, com.tapdata.tm.base.dto.Field fields, UserDetail user);

    public abstract MetadataInstancesDto queryByOne(Filter filter, UserDetail user);

    public abstract List<MetadataInstancesDto> jobStats(long skip, int limit);

    public abstract List<MetadataInstancesDto> schema(Filter filter, UserDetail user);

    public abstract List<MetadataInstancesDto> lienage(String id);

    public abstract void beforeCreateOrUpdate(MetadataInstancesDto data, UserDetail user);

    public abstract void afterFindOne(MetadataInstancesDto result, UserDetail user);

    public abstract void afterFindAll(List<MetadataInstancesDto> results);

    public abstract void afterFind(MetadataInstancesDto metadata);

    public abstract void afterFind(List<MetadataInstancesDto> metadatas);

    public abstract Map<String, Object> classifications(List<ClassificationParam> classificationParamList);

    public abstract void beforeUpdateById(ObjectId id, MetadataInstancesDto data);

    //TODO
    public abstract void afterUpdateById(ObjectId id, MetadataInstancesDto data);

    public abstract MetadataUtil.CompareResult compareHistory(ObjectId id, int historyVersion);

    public abstract List<MetadataInstancesDto> tableConnection(String name, UserDetail user);

    //sam说这个不需要实现
    public abstract void dataMap(String level, String tag, String connectionId, String tableName);

    public abstract List<MetadataInstancesDto> originalData(String isTarget, String qualified_name, UserDetail user);

    public abstract MetadataInstancesDto findBySourceIdAndTableName(String sourceId, String tableName, String taskId, UserDetail userDetail);

    public abstract List<MetadataInstancesDto> findSourceSchemaBySourceId(String sourceId, List<String> tableNames, UserDetail userDetail, String... fields);

    public abstract List<MetadataInstancesDto> findBySourceIdAndTableNameList(String sourceId, List<String> tableNames, UserDetail userDetail, String taskId);

    public abstract List<MetadataInstancesDto> findBySourceIdAndTableNameListNeTaskId(String sourceId, List<String> tableNames, UserDetail userDetail);

    public abstract List<MetadataInstancesEntity> findEntityBySourceIdAndTableNameList(String sourceId, List<String> tableNames, UserDetail userDetail, String taskId);

    public abstract Update buildUpdateSet(MetadataInstancesEntity entity);

    public abstract List<MetadataInstancesDto> findByQualifiedName(String qualifiedName, UserDetail user);

    public abstract MetadataInstancesDto findByQualifiedNameNotDelete(String qualifiedName, UserDetail user, String... fieldName);

    public abstract List<MetadataInstancesDto> findByQualifiedNameList(List<String> qualifiedNames, String taskId);

    public abstract List<MetadataInstancesDto> findByQualifiedNameNotDelete(List<String> qualifiedNames, UserDetail user, String... excludeFiled);

    public abstract List<MetadataInstancesDto> findDatabaseSchemeNoHistory(List<String> databaseIds, UserDetail user);

    public abstract int bulkSave(List<MetadataInstancesDto> metadataInstancesDtos,
                 MetadataInstancesDto dataSourceMetadataInstance,
                 DataSourceConnectionDto dataSourceConnectionDto,
                 DAG.Options options,
                 UserDetail userDetail,
                 Map<String, MetadataInstancesEntity> existsMetadataInstances);

    public abstract int bulkSave(List<MetadataInstancesDto> insertMetaDataDtos,
                 Map<String, MetadataInstancesDto> updateMetaMap, UserDetail userDetail, boolean saveHistory, String taskId, String uuid);

    public abstract Pair<Integer, Integer> bulkUpsetByWhere(List<MetadataInstancesDto> metadataInstancesDtos, UserDetail user);

    public abstract List<String> tables(String connectId, String sourceType);

    public abstract List<Map<String, String>> tableValues(String connectId, String sourceType);

    public abstract Page<Map<String, Object>> pageTables(String connectId, String sourceType, String regex, int skip, int limit);

    public abstract TableSupportInspectVo tableSupportInspect(String connectId, String tableName);

    public abstract List<TableSupportInspectVo> tablesSupportInspect(TablesSupportInspectParam tablesSupportInspectParam);

    public abstract Table getMetadata(String connectionId, String metaType, String tableName, UserDetail user);

    public abstract TapTable getMetadataV2(String connectionId, String metaType, String tableName, UserDetail user);

    public abstract List<Table> findOldByNodeId(Filter filter, UserDetail user);

    public abstract Map<String, String> findTableMapByNodeId(Filter filter);

    public abstract Map<String, String> findKVByNode(String nodeId);

    public abstract String findHeartbeatQualifiedNameByNodeId(Filter filter, UserDetail user);

    public abstract String getQualifiedNameByNodeId(Node node, UserDetail user, DataSourceConnectionDto dataSource, DataSourceDefinitionDto definitionDto, String taskId);

    public abstract List<String> findDatabaseNodeQualifiedName(String nodeId, UserDetail user, TaskDto taskDto, DataSourceConnectionDto dataSource, DataSourceDefinitionDto definitionDto, List<String> includes);

    public abstract List<MetadataInstancesDto> findByNodeId(String nodeId, UserDetail userDetail);

    public abstract List<MetadataInstancesDto> findByNodeId(String nodeId, UserDetail userDetail, String taskId, String... fields);

    public abstract List<MetadataInstancesDto> findByTaskId(String taskId, UserDetail userDetail);

    public abstract List<MetadataInstancesDto> findByNodeId(String nodeId, List<String> fields, UserDetail user, TaskDto taskDto);

    public abstract Map<String, List<MetadataInstancesDto>> findByNodeIds(List<String> nodeIds, List<String> fields, UserDetail user, TaskDto taskDto);

    public abstract Page<MetadataInstancesDto> findByNodeId(String nodeId, List<String> fields, UserDetail user, TaskDto taskDto, String tableFilter, String filterType, int page, int pageSize);

    public abstract List<Map<String, Object>> search(String type, String keyword, String lastId, Integer pageSize, UserDetail user);

    public abstract List<MetaTableVo> tableSearch(String connectionId, String keyword, String lastId, Integer pageSize, UserDetail user);

    public abstract MetaTableCheckVo checkTableNames(String connectionId, List<String> names, UserDetail user);

    public abstract Page findMetadataList(Filter filter, UserDetail userDetail);

    public abstract TableListVo findTablesById(String id);

    public abstract Map<String, MetadataInstancesDto> batchImport(List<MetadataInstancesDto> metadataInstancesDtos, UserDetail user, boolean cover, Map<String, DataSourceConnectionDto> conMap);

    public abstract Page<TapTable> getTapTable(Filter filter, UserDetail loginUser);

    public abstract Page<TapTable> getTapTable(DatabaseNode node, UserDetail loginUser);

    public abstract List<Field> getMergeNodeParentField(String taskId, String nodeId, UserDetail user);

    public abstract void qualifiedNameLinkLogic(List<String> qualifiedNames, UserDetail user);

    public abstract void qualifiedNameLinkLogic(List<String> qualifiedNames, UserDetail user, String taskId);

    //带有taskId的为ddl任务传过来的。所以不需要过滤一些运行状态的任务模型。
    public abstract void linkLogic(List<MetadataInstancesDto> metadataInstancesDtos, UserDetail user, String taskId);

    public abstract void deleteTaskMetadata(String taskId, UserDetail user);

    public abstract Map<String, TapType> dataType2TapType(DataType2TapTypeDto dto, UserDetail user);

    public abstract boolean checkTableExist(String connectionId, String tableName, UserDetail user);

    public abstract void deleteLogicModel(String taskId, String nodeId);

    public abstract long countUpdateExNum(String nodeId);

    public abstract long countTransformExNum(String nodeId);

    public abstract long countTotalNum(String nodeId);

    public abstract void updateTableDesc(MetadataInstancesDto metadataInstances, UserDetail userDetail);

    public abstract void updateTableFieldDesc(String id, DiscoveryFieldDto discoveryFieldDto, UserDetail userDetail);

    public abstract MetadataInstancesDto importEntity(MetadataInstancesDto metadataInstancesDto, UserDetail userDetail);

    public abstract void updateTableCustomDesc(String qualifiedName, String customDesc, UserDetail user);

    public abstract void updateFieldCustomDesc(String qualifiedName, Map<String, String> fieldCustomDescMap, UserDetail user);

    public abstract DataTypeCheckMultipleVo dataTypeCheckMultiple(String databaseType, String dataType, UserDetail user);

    public abstract Set<String> getTypeFilter(String nodeId, UserDetail userDetail);

    public abstract MetadataInstancesDto multiTransform(MultiPleTransformReq multiPleTransformReq, UserDetail user);

    public abstract Boolean checkMetadataInstancesIndex(String cacheKeys, String id);

    public abstract void setUserService(com.tapdata.tm.user.service.UserService userService);

    public abstract void setTaskService(com.tapdata.tm.task.service.TaskService taskService);

    public abstract void setMongoTemplate(org.springframework.data.mongodb.core.MongoTemplate mongoTemplate);

    public abstract Map<String,Long> checkSetLastUpdate(List<MetadataInstancesDto> insertMetaDataDtos);

    public abstract Long getDatabaseMetadataInstanceLastUpdate(String connectionId,UserDetail user);
}
