package com.tapdata.tm.metadatainstance.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.IBaseService;
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
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
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
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Update;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MetadataInstancesService extends IBaseService<MetadataInstancesDto, MetadataInstancesEntity, ObjectId, MetadataInstancesRepository> {
    MetadataInstancesDto add(MetadataInstancesDto record, UserDetail user);

    MetadataInstancesDto modifyById(ObjectId id, MetadataInstancesDto record, UserDetail user);

    Page<MetadataInstancesDto> list(Filter filter, UserDetail user);

    List<MetadataInstancesVo> findInspect(Filter filter, UserDetail userDetail);

    MetadataInstancesDto queryById(ObjectId id, com.tapdata.tm.base.dto.Field fields, UserDetail user);

    MetadataInstancesDto queryByOne(Filter filter, UserDetail user);

    List<MetadataInstancesDto> jobStats(long skip, int limit);

    List<MetadataInstancesDto> schema(Filter filter, UserDetail user);

    List<MetadataInstancesDto> lienage(String id);

    void beforeCreateOrUpdate(MetadataInstancesDto data, UserDetail user);

    void afterFindOne(MetadataInstancesDto result, UserDetail user);

    void afterFindAll(List<MetadataInstancesDto> results);

    void afterFind(MetadataInstancesDto metadata);

    void afterFind(List<MetadataInstancesDto> metadatas);

    Map<String, Object> classifications(List<ClassificationParam> classificationParamList);

    void beforeUpdateById(ObjectId id, MetadataInstancesDto data);

    //TODO
    void afterUpdateById(ObjectId id, MetadataInstancesDto data);

    MetadataUtil.CompareResult compareHistory(ObjectId id, int historyVersion);

    List<MetadataInstancesDto> tableConnection(String name, UserDetail user);

    //sam说这个不需要实现
    void dataMap(String level, String tag, String connectionId, String tableName);

    List<MetadataInstancesDto> originalData(String isTarget, String qualified_name, UserDetail user);

    MetadataInstancesDto findBySourceIdAndTableName(String sourceId, String tableName, String taskId, UserDetail userDetail);

    List<MetadataInstancesDto> findSourceSchemaBySourceId(String sourceId, List<String> tableNames, UserDetail userDetail, String... fields);

    List<MetadataInstancesDto> findBySourceIdAndTableNameList(String sourceId, List<String> tableNames, UserDetail userDetail, String taskId);

    List<MetadataInstancesDto> findBySourceIdAndTableNameListNeTaskId(String sourceId, List<String> tableNames, UserDetail userDetail);

    List<MetadataInstancesEntity> findEntityBySourceIdAndTableNameList(String sourceId, List<String> tableNames, UserDetail userDetail, String taskId);

    Update buildUpdateSet(MetadataInstancesEntity entity);

    List<MetadataInstancesDto> findByQualifiedName(String qualifiedName, UserDetail user);

    MetadataInstancesDto findByQualifiedNameNotDelete(String qualifiedName, UserDetail user, String... fieldName);

    List<MetadataInstancesDto> findByQualifiedNameList(List<String> qualifiedNames, String taskId);

    List<MetadataInstancesDto> findByQualifiedNameNotDelete(List<String> qualifiedNames, UserDetail user, String... excludeFiled);

    List<MetadataInstancesDto> findDatabaseSchemeNoHistory(List<String> databaseIds, UserDetail user);

    int bulkSave(List<MetadataInstancesDto> metadataInstancesDtos,
                 MetadataInstancesDto dataSourceMetadataInstance,
                 DataSourceConnectionDto dataSourceConnectionDto,
                 DAG.Options options,
                 UserDetail userDetail,
                 Map<String, MetadataInstancesEntity> existsMetadataInstances);

    int bulkSave(List<MetadataInstancesDto> insertMetaDataDtos,
                 Map<String, MetadataInstancesDto> updateMetaMap, UserDetail userDetail, boolean saveHistory, String taskId, String uuid);

    Pair<Integer, Integer> bulkUpsetByWhere(List<MetadataInstancesDto> metadataInstancesDtos, UserDetail user);

    List<String> tables(String connectId, String sourceType);

    List<Map<String, String>> tableValues(String connectId, String sourceType);

    Page<Map<String, Object>> pageTables(String connectId, String sourceType, String regex, int skip, int limit);

    TableSupportInspectVo tableSupportInspect(String connectId, String tableName);

    List<TableSupportInspectVo> tablesSupportInspect(TablesSupportInspectParam tablesSupportInspectParam);

    Table getMetadata(String connectionId, String metaType, String tableName, UserDetail user);

    TapTable getMetadataV2(String connectionId, String metaType, String tableName, UserDetail user);

    List<Table> findOldByNodeId(Filter filter, UserDetail user);

    Map<String, String> findTableMapByNodeId(Filter filter);

    Map<String, String> findKVByNode(String nodeId);

    String findHeartbeatQualifiedNameByNodeId(Filter filter, UserDetail user);

    String getQualifiedNameByNodeId(Node node, UserDetail user, DataSourceConnectionDto dataSource, DataSourceDefinitionDto definitionDto, String taskId);

    List<String> findDatabaseNodeQualifiedName(String nodeId, UserDetail user, TaskDto taskDto, DataSourceConnectionDto dataSource, DataSourceDefinitionDto definitionDto, List<String> includes);

    List<MetadataInstancesDto> findByNodeId(String nodeId, UserDetail userDetail);

    List<MetadataInstancesDto> findByNodeId(String nodeId, UserDetail userDetail, String taskId, String... fields);

    List<MetadataInstancesDto> findByTaskId(String taskId, UserDetail userDetail);

    List<MetadataInstancesDto> findByNodeId(String nodeId, List<String> fields, UserDetail user, TaskDto taskDto);

    Map<String, List<MetadataInstancesDto>> findByNodeIds(List<String> nodeIds, List<String> fields, UserDetail user, TaskDto taskDto);

    Page<MetadataInstancesDto> findByNodeId(String nodeId, List<String> fields, UserDetail user, TaskDto taskDto, String tableFilter, String filterType, int page, int pageSize);

    List<Map<String, Object>> search(String type, String keyword, String lastId, Integer pageSize, UserDetail user);

    List<MetaTableVo> tableSearch(String connectionId, String keyword, String lastId, Integer pageSize, UserDetail user);

    MetaTableCheckVo checkTableNames(String connectionId, List<String> names, UserDetail user);

    Page findMetadataList(Filter filter, UserDetail userDetail);

    TableListVo findTablesById(String id);

    Map<String, MetadataInstancesDto> batchImport(List<MetadataInstancesDto> metadataInstancesDtos, UserDetail user, boolean cover, Map<String, DataSourceConnectionDto> conMap);

    Page<TapTable> getTapTable(Filter filter, UserDetail loginUser);

    Page<TapTable> getTapTable(DatabaseNode node, UserDetail loginUser);

    List<Field> getMergeNodeParentField(String taskId, String nodeId, UserDetail user);

    void qualifiedNameLinkLogic(List<String> qualifiedNames, UserDetail user);

    void qualifiedNameLinkLogic(List<String> qualifiedNames, UserDetail user, String taskId);

    //带有taskId的为ddl任务传过来的。所以不需要过滤一些运行状态的任务模型。
    void linkLogic(List<MetadataInstancesDto> metadataInstancesDtos, UserDetail user, String taskId);

    void deleteTaskMetadata(String taskId, UserDetail user);

    Map<String, TapType> dataType2TapType(DataType2TapTypeDto dto, UserDetail user);

    boolean checkTableExist(String connectionId, String tableName, UserDetail user);

    void deleteLogicModel(String taskId, String nodeId);

    long countUpdateExNum(String nodeId);

    long countTransformExNum(String nodeId);

    long countTotalNum(String nodeId);

    void updateTableDesc(MetadataInstancesDto metadataInstances, UserDetail userDetail);

    void updateTableFieldDesc(String id, DiscoveryFieldDto discoveryFieldDto, UserDetail userDetail);

    MetadataInstancesDto importEntity(MetadataInstancesDto metadataInstancesDto, UserDetail userDetail);

    void updateTableCustomDesc(String qualifiedName, String customDesc, UserDetail user);

    void updateFieldCustomDesc(String qualifiedName, Map<String, String> fieldCustomDescMap, UserDetail user);

    DataTypeCheckMultipleVo dataTypeCheckMultiple(String databaseType, String dataType, UserDetail user);

    Set<String> getTypeFilter(String nodeId, UserDetail userDetail);

    MetadataInstancesDto multiTransform(MultiPleTransformReq multiPleTransformReq, UserDetail user);

    Boolean checkMetadataInstancesIndex(String cacheKeys, String id);

    void setUserService(com.tapdata.tm.user.service.UserService userService);

    void setTaskService(com.tapdata.tm.task.service.TaskService taskService);

    void setMongoTemplate(org.springframework.data.mongodb.core.MongoTemplate mongoTemplate);
}
