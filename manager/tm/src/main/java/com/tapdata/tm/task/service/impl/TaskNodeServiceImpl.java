package com.tapdata.tm.task.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.google.common.collect.Maps;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.process.script.MigrateScriptProcessNode;
import com.tapdata.tm.commons.dag.process.script.ScriptProcessNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.dag.vo.TestRunDto;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.*;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.task.service.TaskNodeService;
import com.tapdata.tm.task.service.TaskRecordService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.utils.CacheUtils;
import com.tapdata.tm.task.vo.JsResultDto;
import com.tapdata.tm.task.vo.JsResultVo;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.OEMReplaceUtil;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.fromJson;
import static io.tapdata.entity.simplify.TapSimplify.map;

@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskNodeServiceImpl implements TaskNodeService {

    private TaskService taskService;
    private MetadataInstancesService metadataInstancesService;
    private DataSourceService dataSourceService;
    private MessageQueueService messageQueueService;
    private WorkerService workerService;
    private DataSourceDefinitionService dataSourceDefinitionService;
    private TaskRecordService taskRecordService;
    private MonitoringLogsService monitoringLogService;
    private DAGDataService dagDataService;

    @SneakyThrows
    @Override
    public Page<MetadataTransformerItemDto> getNodeTableInfo(String taskId, String taskRecordId, String nodeId,
                                                             String searchTableName,
                                                             Integer page, Integer pageSize, UserDetail userDetail) {
        Page<MetadataTransformerItemDto> result = new Page<>();

        AtomicReference<TaskDto> taskDto = new AtomicReference<>();
        FunctionUtils.isTureOrFalse(StringUtils.isBlank(taskRecordId)).trueOrFalseHandle(
                () -> taskDto.set(taskService.findById(MongoUtils.toObjectId(taskId))),
                () -> taskDto.set(taskRecordService.queryTask(taskRecordId, userDetail.getUserId()))
        );

        DAG dag = taskDto.get().getDag();
        if (CollectionUtils.isEmpty(dag.getEdges()) ||
                Objects.isNull(dag.getPreNodes(nodeId))) {
            return result;
        }

        if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.get().getSyncType())) {
            if (CollectionUtils.isEmpty(dag.getSourceNode())) {
                return result;
            }

            return getNodeInfoByMigrate(taskId, nodeId, searchTableName, page, pageSize, userDetail, result, dag);
        } else if (TaskDto.SYNC_TYPE_SYNC.equals(taskDto.get().getSyncType())) {

            List<MetadataInstancesDto> metadataInstancesDtos = metadataInstancesService.findByNodeId(nodeId, userDetail);
            if (CollectionUtils.isEmpty(metadataInstancesDtos)) {
                AtomicInteger times = new AtomicInteger();
                while (times.get() < 11) {
                    metadataInstancesDtos = metadataInstancesService.findByNodeId(nodeId, userDetail);
                    Thread.sleep(1000);
                    times.incrementAndGet();
                }
            }

            if (CollectionUtils.isNotEmpty(metadataInstancesDtos)) {
                List<MetadataTransformerItemDto> data = Lists.newArrayList();
                for (MetadataInstancesDto instance : metadataInstancesDtos) {
                    MetadataTransformerItemDto item = new MetadataTransformerItemDto();
                    item.setSourceObjectName(instance.getOriginalName());
                    item.setPreviousTableName(instance.getOriginalName());
                    item.setSinkObjectName(instance.getName());
                    item.setSinkQulifiedName(instance.getQualifiedName());

                    data.add(item);
                }
                result.setItems(data);
            }

            result.setTotal(1);
            return result;
        } else {
            return result;
        }
    }

    private Page<MetadataTransformerItemDto> getNodeInfoByMigrate(String taskId, String nodeId, String searchTableName, Integer page, Integer pageSize, UserDetail userDetail, Page<MetadataTransformerItemDto> result, DAG dag) {
        DatabaseNode sourceNode = dag.getSourceNode(nodeId);
        if (Objects.isNull(sourceNode)) {
            return result;
        }
        DatabaseNode targetNode = CollectionUtils.isNotEmpty(dag.getTargetNode()) ? dag.getTargetNode(nodeId) : null;
        List<String> tableNames = sourceNode.getTableNames();
        if (StringUtils.equals("expression", sourceNode.getMigrateTableSelectType())) {
            List<MetadataInstancesDto> metaInstances = metadataInstancesService.findSourceSchemaBySourceId(sourceNode.getConnectionId(), null, userDetail, "original_name");
//            if (CollectionUtils.isEmpty(metaInstances)) {
//                metaInstances = metadataInstancesService.findBySourceIdAndTableNameListNeTaskId(sourceNode.getConnectionId(), null, userDetail);
//            }
					Function<MetadataInstancesDto, Boolean> filterTableByNoPrimaryKey = Optional
						.of(NoPrimaryKeyTableSelectType.parse(sourceNode.getNoPrimaryKeyTableSelectType()))
						.map(type -> {
							switch (type) {
								case HasKeys:
									return (Function<MetadataInstancesDto, Boolean>) metadataInstancesDto -> {
										if (null != metadataInstancesDto.getFields()) {
											for (Field field : metadataInstancesDto.getFields()) {
												if (Boolean.TRUE.equals(field.getPrimaryKey())) return false;
											}
										}
										return true;
									};
								case NoKeys:
									return (Function<MetadataInstancesDto, Boolean>) metadataInstancesDto -> {
										if (null != metadataInstancesDto.getFields()) {
											for (Field field : metadataInstancesDto.getFields()) {
												if (Boolean.TRUE.equals(field.getPrimaryKey())) return true;
											}
										}
										return false;
									};
								default:
							}
							return null;
						}).orElse(metadataInstancesDto -> false);

            tableNames = metaInstances.stream()
							.map(metadataInstancesDto -> {
								if (filterTableByNoPrimaryKey.apply(metadataInstancesDto)) {
									return null;
								}
								return metadataInstancesDto.getOriginalName();
							})
                    .filter(originalName -> {
											if (null == originalName) {
												return false;
											} else if (StringUtils.isEmpty(sourceNode.getTableExpression())) {
                            return false;
                        } else {
                            return Pattern.matches(sourceNode.getTableExpression(), originalName);
                        }
                    })
                    .collect(Collectors.toList());
        }

        List<String> currentTableList = Lists.newArrayList();
        if (StringUtils.isNotBlank(searchTableName)) {
            currentTableList.add(searchTableName);
            tableNames = tableNames.stream().filter(s -> s.contains(searchTableName)).collect(Collectors.toList());
        }

        if (CollectionUtils.isEmpty(tableNames)) {
            return result;
        }

        currentTableList = ListUtils.partition(tableNames, pageSize).get(page - 1);

        DataSourceConnectionDto targetDataSource;
        if (targetNode != null) {
            targetDataSource = dataSourceService.findById(MongoUtils.toObjectId(targetNode.getConnectionId()));
        } else {
            targetNode = sourceNode;
            targetDataSource = dataSourceService.findById(MongoUtils.toObjectId(sourceNode.getConnectionId()));
        }

        List<Node<?>> predecessors = dag.nodeMap().get(nodeId);
        Node<?> currentNode = dag.getNode(nodeId);
        if (CollectionUtils.isEmpty(predecessors)) {
            predecessors = Lists.newArrayList();
        }
        predecessors.add(currentNode);

        // if current node pre has js node need get data from metaInstances
        boolean preHasJsNode = dag.getPreNodes(nodeId).stream().anyMatch(n -> n instanceof MigrateJsProcessorNode);
        if (preHasJsNode)
            return getMetaByJsNode(nodeId, result, sourceNode, targetNode, tableNames, currentTableList, targetDataSource, predecessors, taskId, userDetail);
        else
            return getMetadataTransformerItemDtoPage(userDetail, result, sourceNode, targetNode, tableNames, currentTableList, targetDataSource, taskId, predecessors, currentNode);
    }

    private Page<MetadataTransformerItemDto> getMetaByJsNode(String nodeId, Page<MetadataTransformerItemDto> result, DatabaseNode sourceNode, DatabaseNode targetNode, List<String> tableNames, List<String> currentTableList, DataSourceConnectionDto targetDataSource, List<Node<?>> predecessors, String taskId, UserDetail user) {
        // table rename
        LinkedList<TableRenameProcessNode> tableRenameProcessNodes = predecessors.stream()
                .filter(node -> node instanceof TableRenameProcessNode)
                .map(node -> (TableRenameProcessNode) node)
                .collect(Collectors.toCollection(LinkedList::new));
        Map<String, TableRenameTableInfo> tableNameMapping = null;
        if (CollectionUtils.isNotEmpty(tableRenameProcessNodes)) {
            tableNameMapping = tableRenameProcessNodes.getLast().originalMap();
        }

        Node currentNode = null;
        Node previousNode = null;
        if(CollectionUtils.isNotEmpty(predecessors)) {
            currentNode = predecessors.get(predecessors.size() - 1);
            if (predecessors.size() - 2 >= 0) {
                previousNode = predecessors.get(predecessors.size() - 2);
            }
        }

        Map<String, Map<String, Boolean>> mappingMap = new HashMap<>();
        if (currentNode != null) {
            if (currentNode instanceof MigrateFieldRenameProcessorNode) {
                LinkedList<TableFieldInfo> fieldsMapping = ((MigrateFieldRenameProcessorNode) currentNode).getFieldsMapping();
                if (fieldsMapping != null) {
                    for (TableFieldInfo tableFieldInfo : fieldsMapping) {
                        LinkedList<FieldInfo> fields = tableFieldInfo.getFields();
                        Map<String, Boolean> fieldMap = fields.stream().collect(Collectors.toMap(FieldInfo::getSourceFieldName, FieldInfo::getIsShow));
                        mappingMap.put(tableFieldInfo.getOriginTableName(), fieldMap);
                    }
                }
            }
        }


        String metaType = "mongodb".equals(targetDataSource.getDatabase_type()) ? "collection" : "table";
        List<String> qualifiedNames = Lists.newArrayList();
        for (String tableName : currentTableList) {
            String tempName;
            if (Objects.nonNull(tableNameMapping) && !tableNameMapping.isEmpty() && Objects.nonNull(tableNameMapping.get(tableName))) {
                tempName = tableNameMapping.get(tableName).getCurrentTableName();
            } else {
                tempName = tableName;
            }

            if (targetNode != null && nodeId.equals(targetNode.getId())) {
                qualifiedNames.add(MetaDataBuilderUtils.generateQualifiedName(metaType, targetDataSource, tempName, taskId));
            } else {
                qualifiedNames.add(MetaDataBuilderUtils.generateQualifiedName(MetaType.processor_node.name(), nodeId, tempName, taskId));
            }
        }

        Map<String, String> sourceMetaMap = new HashMap<>();
        if (sourceNode != null && StringUtils.isNotBlank(sourceNode.getId())) {
            List<MetadataInstancesDto> sourceMetas = metadataInstancesService.findByNodeId(sourceNode.getId(), user, taskId);
            if (CollectionUtils.isNotEmpty(sourceMetas)) {
                sourceMetaMap = sourceMetas.stream().collect(Collectors.
                        toMap(MetadataInstancesDto::getAncestorsName, MetadataInstancesDto::getQualifiedName, (k1, k2) -> k1));
            }
        }
        Map<String, String> targetMetaMap = new HashMap<>();
        if (targetNode != null && StringUtils.isNotBlank(targetNode.getId())) {
            List<MetadataInstancesDto> targetMetas = metadataInstancesService.findByNodeId(targetNode.getId(), user, taskId);
            if (CollectionUtils.isNotEmpty(targetMetas)) {
                targetMetaMap = targetMetas.stream().collect(Collectors.
                        toMap(MetadataInstancesDto::getAncestorsName, MetadataInstancesDto::getQualifiedName, (k1, k2) -> k1));
            }
        }


        List<MetadataInstancesDto> instances = metadataInstancesService.findByQualifiedNameList(qualifiedNames, taskId);
        Map<String, String> previousTableNameMap = new HashMap<>();
        if (previousNode != null) {
            List<MetadataInstancesDto> lastMetas = metadataInstancesService.findByNodeId(previousNode.getId(), user, taskId, "ancestorsName", "original_name");
            previousTableNameMap = lastMetas.stream().collect(Collectors.toMap(k -> k.getAncestorsName(), v -> v.getOriginalName(), (k1, k2) -> k1));
        }
        if (CollectionUtils.isNotEmpty(instances)) {
            List<MetadataTransformerItemDto> data = Lists.newArrayList();
            for (MetadataInstancesDto instance : instances) {
                MetadataTransformerItemDto item = new MetadataTransformerItemDto();
                item.setSourceObjectName(instance.getAncestorsName());
                String previousTableName = previousTableNameMap.get(instance.getAncestorsName());
                item.setPreviousTableName( previousTableName == null ? instance.getAncestorsName() : previousTableName);
                item.setSinkObjectName(instance.getName());
                item.setSinkQulifiedName(targetMetaMap.get(instance.getAncestorsName()));
                item.setSourceQualifiedName(sourceMetaMap.get(instance.getAncestorsName()));

                List<FieldsMapping> fieldsMapping = Lists.newArrayList();
                List<Field> fields = instance.getFields().stream().sorted(Comparator.comparing(Field::getColumnPosition)).collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(fields)) {
                    Map<String, Boolean> fieldMap = mappingMap.get(instance.getAncestorsName());
                    for (Field field : fields) {
//                        if (field.isDeleted()) {
//                            continue;
//                        }
                        String defaultValue = Objects.isNull(field.getDefaultValue()) ? "" : field.getDefaultValue().toString();
                        int primaryKey = Objects.isNull(field.getPrimaryKeyPosition()) ? 0 : field.getPrimaryKeyPosition();

                        FieldsMapping mapping = new FieldsMapping(){{
                            setTargetFieldName(field.getFieldName());
                            setSourceFieldName(field.getOriginalFieldName());
                            setSourceFieldType(field.getDataType());
                            setType("auto");
                            setDefaultValue(defaultValue);
                            setIsShow(true);
                            if (fieldMap != null) {
                                Boolean show = fieldMap.get(field.getOriginalFieldName());
                                if (show != null) {
                                    setIsShow(show);
                                }
                            }
                            setMigrateType("system");
                            setPrimary_key_position(primaryKey);
                            setUseDefaultValue(field.getUseDefaultValue());
                        }};
                        fieldsMapping.add(mapping);
                    }
                }

                item.setFieldsMapping(fieldsMapping);
                item.setSourceFieldCount(fieldsMapping.size());
                item.setSourceDataBaseType(sourceNode.getDatabaseType());
                item.setSinkDbType(targetNode != null ? targetNode.getDatabaseType() : null);

                data.add(item);
            }
            result.setTotal(tableNames.size());
            result.setItems(data);
        }
        return result;
    }

    @NotNull
    private Page<MetadataTransformerItemDto> getMetadataTransformerItemDtoPage(UserDetail userDetail
            , Page<MetadataTransformerItemDto> result, DatabaseNode sourceNode, DatabaseNode targetNode
            , List<String> tableNames, List<String> currentTableList, DataSourceConnectionDto targetDataSource
            , final String taskId, List<Node<?>> predecessors, Node<?> currentNode) {
        if (CollectionUtils.isEmpty(predecessors)) {
            predecessors = Lists.newArrayList();
        }
        predecessors.add(currentNode);

        // table rename
        LinkedList<TableRenameProcessNode> tableRenameProcessNodes = predecessors
                .stream()
                .filter(node -> node instanceof TableRenameProcessNode)
                .map(node -> (TableRenameProcessNode) node)
                .collect(Collectors.toCollection(LinkedList::new));
        Map<String, TableRenameTableInfo> tableNameMapping = null;
        if (CollectionUtils.isNotEmpty(tableRenameProcessNodes)) {
            tableNameMapping = tableRenameProcessNodes.getLast().originalMap();
        }
        // field rename
        LinkedList<MigrateFieldRenameProcessorNode> fieldRenameProcessorNodes = predecessors
                .stream()
                .filter(node -> node instanceof MigrateFieldRenameProcessorNode)
                .map(node -> (MigrateFieldRenameProcessorNode) node)
                .collect(Collectors.toCollection(LinkedList::new));
        Map<String, LinkedList<FieldInfo>> tableFieldMap = null;
        if (CollectionUtils.isNotEmpty(fieldRenameProcessorNodes)) {
            LinkedList<TableFieldInfo> fieldsMapping = fieldRenameProcessorNodes.getLast().getFieldsMapping();
            if (CollectionUtils.isNotEmpty(fieldsMapping)) {
                tableFieldMap = fieldsMapping.stream()
                        .filter(f -> Objects.nonNull(f.getQualifiedName()))
                        .collect(Collectors.toMap(TableFieldInfo::getOriginTableName, TableFieldInfo::getFields, (e1, e2) -> e1));
            }
        }

        DataSourceConnectionDto sourceDataSource = dataSourceService.findById(MongoUtils.toObjectId(sourceNode.getConnectionId()));

        Map<String, MetadataInstancesDto> metaMap = Maps.newHashMap();
        List<MetadataInstancesDto> list = metadataInstancesService.findByNodeId(currentNode.getId(), userDetail);
        boolean queryFormSource = false;
        if (CollectionUtils.isEmpty(list) || list.size() != tableNames.size()) {
            // 可能有这种场景， node detail接口请求比模型加载快，会查不到逻辑表的数据
            list = metadataInstancesService.findBySourceIdAndTableNameListNeTaskId(sourceNode.getConnectionId(),
                    currentTableList, userDetail);
            queryFormSource = true;
        }
        if (CollectionUtils.isNotEmpty(list)) {
            boolean finalQueryFormSource = queryFormSource;
            metaMap = list.stream()
                    .filter(t -> currentTableList.contains(t.getAncestorsName()))
                    .map(meta -> {
                // source & target not same database type and query from source
                if (finalQueryFormSource && currentNode instanceof DatabaseNode
                        && !sourceDataSource.getDatabase_type().equals(targetDataSource.getDatabase_type())) {
                    Schema schema = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(meta), Schema.class);
                    return processFieldToDB(schema, meta, targetDataSource, userDetail);
                } else {
                    return meta;
                }
            }).collect(Collectors.toMap(MetadataInstancesDto::getAncestorsName, Function.identity(), (e1,e2)->e2));
        }

        if (metaMap.isEmpty()) {
            return result;
        }

        List<MetadataTransformerItemDto> data = Lists.newArrayList();
        for (String tableName : currentTableList) {
            if (metaMap.get(tableName) == null) {
                continue;
            }

            MetadataInstancesDto metadataInstancesDto = metaMap.get(tableName);

            MetadataTransformerItemDto item = new MetadataTransformerItemDto();
            item.setSourceObjectName(tableName);
            String sinkTableName = metadataInstancesDto.getOriginalName();
            String previousTableName = metadataInstancesDto.getOriginalName();
            if (Objects.nonNull(tableNameMapping) && !tableNameMapping.isEmpty() && Objects.nonNull(tableNameMapping.get(tableName))) {
                sinkTableName = tableNameMapping.get(tableName).getCurrentTableName();
                previousTableName = tableNameMapping.get(tableName).getPreviousTableName();
            }
            item.setSinkObjectName(sinkTableName);

            List<FieldsMapping> fieldsMapping = new LinkedList<>();
            // set qualifiedName
            String sinkQualifiedName = null;
            if (Objects.nonNull(targetDataSource)) {
                //TODO 现在的mongodb表也是table的，所以这个逻辑是有问题的，但是由于现在的mongodb在库里的类型不是mongodb所以也不会出错。要改的时候，需要改所有类似的的地方
                String metaType = "mongodb".equals(targetDataSource.getDatabase_type()) ? "collection" : "table";
                sinkQualifiedName = MetaDataBuilderUtils.generateQualifiedName(metaType, targetDataSource, tableName, taskId);
            }
            String metaType = "mongodb".equals(sourceDataSource.getDatabase_type()) ? "collection" : "table";
            String sourceQualifiedName = MetaDataBuilderUtils.generateQualifiedName(metaType, sourceDataSource, tableName, taskId);

            // || CollectionUtils.isEmpty(metaMap.get(tableName).getFields())

            List<Field> fields = metadataInstancesDto.getFields().stream()
                    .sorted((Field f1, Field f2) ->{
                        int f1pos = f1.getColumnPosition() == null ? -1 : f1.getColumnPosition();
                        int f2pos = f2.getColumnPosition() == null ? -1 : f2.getColumnPosition();
                        if (f1pos >= f2pos) {
                            return 1;
                        }
                        return -1;
                    })
                    .collect(Collectors.toList());

            // TableRenameProcessNode not need fields
            if (!(currentNode instanceof TableRenameProcessNode)) {
                Map<String, FieldInfo> fieldInfoMap = null;
                if (Objects.nonNull(tableFieldMap) && !tableFieldMap.isEmpty() && tableFieldMap.containsKey(tableName)) {
                    fieldInfoMap = tableFieldMap.get(tableName).stream()
                            .filter(f -> Objects.nonNull(f.getSourceFieldName()))
                            .collect(Collectors.toMap(FieldInfo::getSourceFieldName, Function.identity()));
                }
                for (Field field : fields) {
//                    if (field.isDeleted()) {
//                        continue;
//                    }
                    String defaultValue = Objects.isNull(field.getDefaultValue()) ? "" : field.getDefaultValue().toString();
                    if (StringUtils.isBlank(defaultValue) && field.getUseDefaultValue()) {
                        defaultValue = Objects.isNull(field.getOriginalDefaultValue()) ? "" : field.getOriginalDefaultValue().toString();
                    }
                    int primaryKey = Objects.isNull(field.getPrimaryKeyPosition()) ? 0 : field.getPrimaryKeyPosition();
                    String previousFieldName = field.getPreviousFieldName();
                    String fieldName = field.getFieldName();
                    String finalDefaultValue = defaultValue;
                    FieldsMapping mapping = FieldsMapping.builder()
                            .targetFieldName(fieldName)
                            .sourceFieldName(previousFieldName)
                            .sourceFieldType(field.getDataType())
                            .type("auto")
                            .isShow(true)
                            .migrateType("system")
                            .primary_key_position(primaryKey)
                            .useDefaultValue(field.getUseDefaultValue())
                            .defaultValue(finalDefaultValue).build();

                    if (Objects.nonNull(fieldInfoMap) && fieldInfoMap.containsKey(mapping.getSourceFieldName())) {
                        FieldInfo fieldInfo = fieldInfoMap.get(mapping.getSourceFieldName());

                        if (!(currentNode instanceof MigrateFieldRenameProcessorNode) && !fieldInfo.getIsShow()) {
                            continue;
                        }

                        mapping.setIsShow(fieldInfo.getIsShow());
                        mapping.setMigrateType(fieldInfo.getType());
                        mapping.setTargetFieldName(fieldInfo.getTargetFieldName());
                    }
                    fieldsMapping.add(mapping);
                }
            }

            item.setPreviousTableName(previousTableName);
            item.setSinkQulifiedName(sinkQualifiedName);
            item.setSourceQualifiedName(sourceQualifiedName);
            item.setFieldsMapping(fieldsMapping);
            item.setSourceFieldCount(fieldsMapping.size());
            item.setSourceDataBaseType(sourceNode.getDatabaseType());
            item.setSinkDbType(targetNode != null ? targetNode.getDatabaseType() : null);

            data.add(item);
        }

        result.setTotal(tableNames.size());
        result.setItems(data);
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void testRunJsNode(TestRunDto dto, UserDetail userDetail) {
        String taskId = dto.getTaskId();
        String nodeId = dto.getJsNodeId();
        String tableName = dto.getTableName();
        Integer rows = dto.getRows();
        String script = dto.getScript();
        Long version = dto.getVersion();

        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
        String testTaskId = taskDto.getTestTaskId();
        monitoringLogService.deleteLogs(testTaskId);
        DAG dtoDag = taskDto.getDag();
        TaskDto taskDtoCopy = new TaskDto();
        BeanUtils.copyProperties(taskDto, taskDtoCopy);
        taskDtoCopy.setSyncType(TaskDto.SYNC_TYPE_TEST_RUN);
        taskDtoCopy.setStatus(TaskDto.STATUS_WAIT_RUN);
        if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
            DatabaseNode first = dtoDag.getSourceNode().getFirst();
            first.setTableNames(Lists.of(tableName));
            first.setRows(rows);

            Dag build = dtoDag.toDag();
            build = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(build), Dag.class);
            List<Node<?>> nodes = dtoDag.nodeMap().get(nodeId);
            MigrateJsProcessorNode jsNode = (MigrateJsProcessorNode) dtoDag.getNode(nodeId);
            if (StringUtils.isNotBlank(script)) {
                jsNode.setScript(script);
            }
            nodes.add(jsNode);

            Node<?> target = new VirtualTargetNode();
            target.setId(UUID.randomUUID().toString());
            target.setName(target.getId());
            if (CollectionUtils.isNotEmpty(nodes)) {
                nodes.add(target);
            }

            List<Edge> edges = dtoDag.edgeMap().get(nodeId);
            if (CollectionUtils.isNotEmpty(edges)) {
                Edge edge = new Edge(nodeId, target.getId());
                edges.add(edge);
            }

            Objects.requireNonNull(build).setNodes(new LinkedList<Node>(){{addAll(nodes);}});
            build.setEdges(edges);

            DAG temp = DAG.build(build);
            taskDtoCopy.setDag(temp);
        } else if (TaskDto.SYNC_TYPE_SYNC.equals(taskDto.getSyncType())) {
            final List<String> predIds = new ArrayList<>();
            getPrePre(dtoDag.getNode(nodeId), predIds);
            predIds.add(nodeId);
            Dag dag = dtoDag.toDag();
            dag = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dag), Dag.class);
            List<Node> nodes = dag.getNodes();

            Node<?> target = new VirtualTargetNode();
            target.setId(UUID.randomUUID().toString());
            target.setName(target.getId());
            if (CollectionUtils.isNotEmpty(nodes)) {
                nodes = nodes.stream()
                        .peek(n -> {
                            if (n instanceof JsProcessorNode) {
                                ((JsProcessorNode)n).setScript(script);
                            }
                        })
                        .filter(n -> predIds.contains(n.getId()))
                        .peek(n -> {
                            if (n instanceof TableNode) ((TableNode) n).setRows(rows);
                        })
                        .collect(Collectors.toList());
                nodes.add(target);
            }

            List<Edge> edges = dag.getEdges();
            if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(edges)) {
                edges = edges.stream().filter(e -> (predIds.contains(e.getTarget()) || predIds.contains(e.getSource())) && !e.getSource().equals(nodeId)).collect(Collectors.toList());
                Edge edge = new Edge(nodeId, target.getId());
                edges.add(edge);
            }
            dag.setNodes(nodes);
            dag.setEdges(edges);

            DAG build = DAG.build(dag);
            taskDtoCopy.setDag(build);
        }

        taskDtoCopy.setName(taskDto.getName() + "(101)");
        taskDtoCopy.setVersion(version);
        taskDtoCopy.setId(MongoUtils.toObjectId(testTaskId));
        List<Worker> workers = workerService.findAvailableAgentByAccessNode(userDetail, taskDto.getAccessNodeProcessIdList());
        if (CollectionUtils.isEmpty(workers)) {
            throw new BizException("no agent");
        }

        MessageQueueDto queueDto = new MessageQueueDto();
        queueDto.setReceiver(workers.get(0).getProcessId());
        queueDto.setData(taskDtoCopy);
        queueDto.setType(TaskDto.SYNC_TYPE_TEST_RUN);
        messageQueueService.sendMessage(queueDto);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> testRunJsNodeRPC(TestRunDto dto, UserDetail userDetail, int jsType) {
        String taskId = dto.getTaskId();
        String nodeId = dto.getJsNodeId();
        String tableName = dto.getTableName();
        Integer rows = dto.getRows();
        String script = dto.getScript();
        Long version = dto.getVersion();
        int logOutputCount = dto.getLogOutputCount();

        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
        String testTaskId = taskDto.getTestTaskId();
        monitoringLogService.deleteLogs(testTaskId);
        DAG dtoDag = taskDto.getDag();
        TaskDto taskDtoCopy = new TaskDto();
        BeanUtils.copyProperties(taskDto, taskDtoCopy);
        taskDtoCopy.setSyncType(TaskDto.SYNC_TYPE_TEST_RUN);
        taskDtoCopy.setStatus(TaskDto.STATUS_WAIT_RUN);
        if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
            DatabaseNode first = dtoDag.getSourceNode().getFirst();
            first.setTableNames(Lists.of(tableName));
            first.setRows(rows);
            Dag build = dtoDag.toDag();
            build = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(build), Dag.class);
            List<Node<?>> nodes = dtoDag.nodeMap().get(nodeId);
            Node<?> node = dtoDag.getNode(nodeId);
            if (!(node instanceof MigrateScriptProcessNode)) {
               throw new BizException("Processor node is not of expected type. error type: {}", null == node ? "null" : node.getClass().getName());
            }
            MigrateScriptProcessNode jsNode = (MigrateScriptProcessNode) node;
            jsType = Optional.ofNullable(jsNode.getJsType()).orElse(jsType);

            if (StringUtils.isNotBlank(script)) {
                jsNode.setScript(script);
            }
            nodes.add(jsNode);

            Node<?> target = new VirtualTargetNode();
            target.setId(UUID.randomUUID().toString());
            target.setName(target.getId());
            if (CollectionUtils.isNotEmpty(nodes)) {
                nodes.add(target);
            }

            List<Edge> edges = dtoDag.edgeMap().get(nodeId);
            if (CollectionUtils.isNotEmpty(edges)) {
                Edge edge = new Edge(nodeId, target.getId());
                edges.add(edge);
            }

            Objects.requireNonNull(build).setNodes(new LinkedList<Node>(){{addAll(nodes);}});
            build.setEdges(edges);

            DAG temp = DAG.build(build);
            taskDtoCopy.setDag(temp);
        } else if (TaskDto.SYNC_TYPE_SYNC.equals(taskDto.getSyncType())) {
            final List<String> predIds = new ArrayList<>();
            Node<?> node = dtoDag.getNode(nodeId);
            if (node instanceof ScriptProcessNode){
                ScriptProcessNode processorNode = (ScriptProcessNode) node;
                jsType = Optional.ofNullable(processorNode.getJsType()).orElse(jsType);
            }
            getPrePre(node, predIds);
            predIds.add(nodeId);
            Dag dag = dtoDag.toDag();
            dag = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dag), Dag.class);
            List<Node> nodes = dag.getNodes();

            Node<?> target = new VirtualTargetNode();
            target.setId(UUID.randomUUID().toString());
            target.setName(target.getId());
            if (CollectionUtils.isNotEmpty(nodes)) {
                nodes = nodes.stream()
                        .peek(n -> {
                            if (n instanceof ScriptProcessNode) {
                                ((ScriptProcessNode)n).setScript(script);
                            }
                        })
                        .filter(n -> predIds.contains(n.getId()))
                        .peek(n -> {
                            if (n instanceof TableNode) ((TableNode) n).setRows(rows);
                        })
                        .collect(Collectors.toList());
                nodes.add(target);
            }

            List<Edge> edges = dag.getEdges();
            if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(edges)) {
                edges = edges.stream().filter(e -> (predIds.contains(e.getTarget()) || predIds.contains(e.getSource())) && !e.getSource().equals(nodeId)).collect(Collectors.toList());
                Edge edge = new Edge(nodeId, target.getId());
                edges.add(edge);
            }
            dag.setNodes(nodes);
            dag.setEdges(edges);

            DAG build = DAG.build(dag);
            taskDtoCopy.setDag(build);
        }

        taskDtoCopy.setName(taskDto.getName() + "(101)");
        taskDtoCopy.setVersion(version);
        taskDtoCopy.setId(MongoUtils.toObjectId(testTaskId));
        return jsType == 1 ? rpcTestRun(testTaskId, taskDtoCopy, logOutputCount, nodeId) : wsTestRun(userDetail, taskDto, taskDtoCopy);
    }

    private Map<String, Object> rpcTestRun(String testTaskId, TaskDto taskDtoCopy, int logOutputCount, String nodeId){
        // RPC
        String serverPort = CommonUtils.getProperty("tapdata_proxy_server_port", "3000");
        int port;
        try {
            port = Integer.parseInt(serverPort);
        } catch (Exception exception){
            return resultMap(testTaskId, false, "Can't get server port.");
        }
        Map.Entry<String, Map<String, Object>> attributes = getLoginUserAttributes();
        String attributesKey = attributes.getKey();
        Map<String, Object> attributesValue = attributes.getValue();
        String url = "http://localhost:" +
                port +"/api/proxy/call" +
                ("Param".equals(attributesKey) ? "?access_token=" + attributesValue.get("access_token")  : "");
        Map<String, Object> paraMap = new HashMap<>();
        paraMap.put("className", "JSProcessNodeTestRunService");
        paraMap.put("method", "testRun");
        paraMap.put("args", new ArrayList<Object>(){{ add(taskDtoCopy); add(nodeId); add(logOutputCount); }});

        try {
            OkHttpClient client = new OkHttpClient.Builder()
                    .connectTimeout(90, TimeUnit.SECONDS)
                    .readTimeout(90, TimeUnit.SECONDS)
                    .build();
            Request.Builder post = new Request.Builder()
                    .url(url)
                    .method("POST", RequestBody.create(MediaType.parse("application/json"), JsonUtil.toJsonUseJackson(paraMap)))
                    .addHeader("Content-Type", "application/json");
            if ("Header".equals(attributesKey) && null != attributesValue && !attributesValue.isEmpty()){
                for (Map.Entry<String, Object> entry : attributesValue.entrySet()) {
                    post.addHeader(entry.getKey(), String.valueOf(entry.getValue()));
                }
            }
            Request request = post.build();
            Call call = client.newCall(request);
            Response response = call.execute();
            int code = response.code();
            return 200 >= code && code < 300 ?
                    (Map<String, Object>) fromJson(OEMReplaceUtil.replace(response.body().string(), "connector/replace.json"))
                    : resultMap(testTaskId, false, "Access remote service error, http code: " + code);
        }catch (Exception e){
            return resultMap(testTaskId, false, e.getMessage());
        }
    }

    private Map<String, Object> wsTestRun(UserDetail userDetail, TaskDto taskDto, TaskDto taskDtoCopy){
        // WS
        List<Worker> workers = workerService.findAvailableAgentByAccessNode(userDetail, taskDto.getAccessNodeProcessIdList());
        if (CollectionUtils.isEmpty(workers)) {
            throw new BizException("no agent");
        }

        MessageQueueDto queueDto = new MessageQueueDto();
        queueDto.setReceiver(workers.get(0).getProcessId());
        queueDto.setData(taskDtoCopy);
        queueDto.setType(TaskDto.SYNC_TYPE_TEST_RUN);
        messageQueueService.sendMessage(queueDto);
        return new HashMap<>();
    }

    private Map.Entry<String, Map<String, Object>> getLoginUserAttributes() {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
        HttpServletRequest request = attributes.getRequest();

        String userIdFromHeader = request.getHeader("user_id");
        Map<String, Object> ent = new HashMap<>();
        if (!com.tapdata.manager.common.utils.StringUtils.isBlank(userIdFromHeader)) {
            ent.put("user_id", userIdFromHeader);
            return new AbstractMap.SimpleEntry<>("Header", ent);
        } else if((request.getQueryString() != null ? request.getQueryString() : "").contains("access_token")) {
            Map<String, String> queryMap = Arrays.stream(request.getQueryString().split("&"))
                    .filter(s -> s.startsWith("access_token"))
                    .map(s -> s.split("=")).collect(Collectors.toMap(a -> a[0], a -> {
                        try {
                            return URLDecoder.decode(a[1], "UTF-8");
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                            return a[1];
                        }
                    }, (a, b) -> a));
            String accessToken = queryMap.get("access_token");
            ent.put("access_token", accessToken);
            return new AbstractMap.SimpleEntry<>("Param", ent);
        } else if (request.getHeader("authorization") != null) {
            ent.put("authorization", request.getHeader("authorization").trim());
            return new AbstractMap.SimpleEntry<>("Header", ent);
        } else {
            throw new BizException("NotLogin");
        }
    }


    private Map<String,Object> resultMap(String testTaskId, boolean isSucceed, String message){
        Map<String, Object> errorMap = new HashMap<>();
        errorMap.put("taskId", testTaskId);
        errorMap.put("ts", new Date().getTime());
        errorMap.put("code", isSucceed ? "ok" : "error");
        errorMap.put("message", message);
        return errorMap;
    }

    private void getPrePre(Node node, List<String> preIds) {
        List<Node> predecessors = node.predecessors();
        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(predecessors)) {
            for (Node predecessor : predecessors) {
                preIds.add(predecessor.getId());
                getPrePre(predecessor, preIds);
            }
        }
    }

    @Override
    public void saveResult(JsResultDto jsResultDto) {
        if (Objects.nonNull(jsResultDto)) {
            StringJoiner joiner = new StringJoiner(":");
            joiner.add(jsResultDto.getTaskId());
            joiner.add(jsResultDto.getVersion().toString());
            CacheUtils.put(joiner.toString(),  jsResultDto);
        }
    }

    /**
     * @deprecated 执行试运行后即可获取到试运行结果和试运行日志，无需使用此获取结果，不久的将来会移除这个function
     * */
    @Override
    public ResponseMessage<JsResultVo> getRun(String taskId, String jsNodeId, Long version) {
        ResponseMessage<JsResultVo> res = new ResponseMessage<>();
        JsResultVo result = new JsResultVo();

        TaskDto taskDto = taskService.findByTaskId(MongoUtils.toObjectId(taskId), "testTaskId");

        StringJoiner joiner = new StringJoiner(":");
        joiner.add(taskDto.getTestTaskId());
        joiner.add(version.toString());
        if (CacheUtils.isExist(joiner.toString())) {
            result.setOver(true);
            JsResultDto dto = new JsResultDto();
            BeanUtil.copyProperties(CacheUtils.invalidate(joiner.toString()), dto);
            FunctionUtils.isTureOrFalse(dto.getCode().equals("ok")).trueOrFalseHandle(() -> {
                BeanUtil.copyProperties(dto, result);
                res.setCode("ok");
                res.setData(result);
            }, () -> {
                log.error("getRun JsResultVo error:{}", dto.getMessage());
                res.setCode("SystemError");
                res.setMessage(dto.getMessage());
            });
        } else {
            result.setOver(false);
            res.setData(result);
        }
        return res;
    }

    /**
     * The copy migrate task will have dirty data, which can only be processed in the request details interface
     * @param taskDto taskDto
     * @param userDetail userDetail
     */
    @Override
    public void checkFieldNode(TaskDto taskDto, UserDetail userDetail) {
        if (!taskDto.getName().contains("- Copy")) {
            return;
        }

        String taskId = taskDto.getId().toHexString();

        DAG dag = taskDto.getDag();
        List<String> collect = dag.getNodes().stream().filter(node -> {
            if (node instanceof MigrateFieldRenameProcessorNode) {
                LinkedList<TableFieldInfo> fieldsMapping = ((MigrateFieldRenameProcessorNode) node).getFieldsMapping();

                return fieldsMapping != null && fieldsMapping.stream().anyMatch(table -> !table.getQualifiedName().endsWith(taskId));
            }
            return false;
        }).map(Node::getId)
        .collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(collect) && CollectionUtils.isNotEmpty(dag.getSourceNode())) {
            collect.forEach(nodeId -> {
                MigrateFieldRenameProcessorNode fieldNode = (MigrateFieldRenameProcessorNode) dag.getNode(nodeId);
                fieldNode.getFieldsMapping().forEach(m -> {
                    String qualifiedName = m.getQualifiedName();
                    String pre = qualifiedName.substring(0, qualifiedName.lastIndexOf("_") + 1);
                    m.setQualifiedName(pre + taskId);
                });
            });
        }

    }

    /**
     * 根据字段类型映射规则，将模型 schema中的通用字段类型转换为指定数据库字段类型
     * @param schema 包含通用字段类型的模型
     * @param metadataInstancesDto 将映射后的字段类型保存到这里
     * @param dataSourceConnectionDto 数据库类型
     */
    private MetadataInstancesDto processFieldToDB(Schema schema, MetadataInstancesDto metadataInstancesDto, DataSourceConnectionDto dataSourceConnectionDto, UserDetail user) {

        if (metadataInstancesDto == null || schema == null ||
                metadataInstancesDto.getFields() == null || dataSourceConnectionDto == null){
            log.error("Process field type mapping to db type failed, invalid params: schema={}, metadataInstanceDto={}, dataSourceConnectionsDto={}",
                    schema, metadataInstancesDto, dataSourceConnectionDto);
            return metadataInstancesDto;
        }

        final String databaseType = dataSourceConnectionDto.getDatabase_type();
        String dbVersion = dataSourceConnectionDto.getDb_version();
        if (com.tapdata.manager.common.utils.StringUtils.isBlank(dbVersion)) {
            dbVersion = "*";
        }
        //Map<String, List<TypeMappingsEntity>> typeMapping = typeMappingsService.getTypeMapping(databaseType, TypeMappingDirection.TO_DATATYPE);

        schema.setInvalidFields(new ArrayList<>());
        Map<String, Field> fields = schema.getFields().stream().collect(Collectors.toMap(Field::getFieldName, f -> f, (f1, f2) -> f1));


        DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(databaseType, user);
        String expression = definitionDto.getExpression();
        Map<Class<?>, String> tapMap = definitionDto.getTapMap();

        TapTable tapTable = PdkSchemaConvert.toPdk(schema);


        if (tapTable.getNameFieldMap() != null && tapTable.getNameFieldMap().size() != 0) {
            LinkedHashMap<String, TapField> updateFieldMap = new LinkedHashMap<>();
            tapTable.getNameFieldMap().forEach((k, v) -> {
                if (v.getTapType() == null) {
                    updateFieldMap.put(k, v);
                }
            });

            if (updateFieldMap.size() != 0) {
                PdkSchemaConvert.getTableFieldTypesGenerator().autoFill(updateFieldMap, DefaultExpressionMatchingMap.map(expression));

                updateFieldMap.forEach((k, v) -> tapTable.getNameFieldMap().replace(k, v));
            }
        }

        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();

        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create().withTapTypeDataTypeMap(tapMap));
        TapResult<LinkedHashMap<String, TapField>> convert = PdkSchemaConvert.getTargetTypesGenerator().convert(nameFieldMap
                , DefaultExpressionMatchingMap.map(expression), codecsFilterManager);
        LinkedHashMap<String, TapField> data = convert.getData();

        data.forEach((k, v) -> {
            TapField tapField = nameFieldMap.get(k);
            BeanUtils.copyProperties(v, tapField);
        });
        tapTable.setNameFieldMap(nameFieldMap);



        metadataInstancesDto = PdkSchemaConvert.fromPdk(tapTable);
        metadataInstancesDto.setAncestorsName(schema.getAncestorsName());
        metadataInstancesDto.setNodeId(schema.getNodeId());

        metadataInstancesDto.getFields().forEach(field -> {
            if (field.getId() == null) {
                field.setId(new ObjectId().toHexString());
            }
            Field originalField = fields.get(field.getFieldName());
            if (databaseType.equalsIgnoreCase(field.getSourceDbType())) {
                if (originalField != null && originalField.getDataTypeTemp() != null) {
                    field.setDataType(originalField.getDataTypeTemp());
                }
            }
        });

        Map<String, Field> result = metadataInstancesDto.getFields()
                .stream().collect(Collectors.toMap(Field::getFieldName, m -> m, (m1, m2) -> m2));
        if (result.size() != metadataInstancesDto.getFields().size()) {
            metadataInstancesDto.setFields(new ArrayList<>(result.values()));
        }

        return metadataInstancesDto;
    }
}
