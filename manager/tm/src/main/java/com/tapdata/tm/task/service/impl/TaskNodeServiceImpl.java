package com.tapdata.tm.task.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.google.common.collect.Maps;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.dag.vo.TestRunDto;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.commons.util.MetaType;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.service.TaskNodeService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.utils.CacheUtils;
import com.tapdata.tm.task.vo.JsResultDto;
import com.tapdata.tm.task.vo.JsResultVo;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    @Override
    public Page<MetadataTransformerItemDto> getNodeTableInfo(String taskId, String nodeId, String searchTableName,
                                                             Integer page, Integer pageSize, UserDetail userDetail) {
        Page<MetadataTransformerItemDto> result = new Page<>();

        DAG dag = taskService.findById(MongoUtils.toObjectId(taskId)).getDag();
        if (CollectionUtils.isEmpty(dag.getEdges()) || Objects.isNull(dag.getPreNodes(nodeId))) {
            return result;
        }

        LinkedList<DatabaseNode> databaseNodes = dag.getNodes().stream()
                .filter(node -> node instanceof DatabaseNode)
                .map(node -> (DatabaseNode) node)
                .collect(Collectors.toCollection(LinkedList::new));
        if (CollectionUtils.isEmpty(databaseNodes)) {
            return result;
        }

        DatabaseNode sourceNode = dag.getSourceNode().getFirst();
        DatabaseNode targetNode = CollectionUtils.isNotEmpty(dag.getTargetNode()) ? dag.getTargetNode().getLast() : null;
        List<String> tableNames = sourceNode.getTableNames();
        if (CollectionUtils.isEmpty(tableNames) && StringUtils.equals("all", sourceNode.getMigrateTableSelectType())) {
            List<MetadataInstancesDto> metaInstances = metadataInstancesService.findBySourceIdAndTableNameList(sourceNode.getConnectionId(), null, userDetail);
            tableNames = metaInstances.stream().map(MetadataInstancesDto::getOriginalName).collect(Collectors.toList());
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

        DataSourceConnectionDto targetDataSource = null;
        if (targetNode != null) {
            targetDataSource = dataSourceService.findById(MongoUtils.toObjectId(targetNode.getConnectionId()));
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
            return getMetaByJsNode(nodeId, result, sourceNode, targetNode, tableNames, currentTableList, targetDataSource, predecessors);
        else
            return getMetadataTransformerItemDtoPage(userDetail, result, sourceNode, targetNode, tableNames, currentTableList, targetDataSource, predecessors, currentNode);
    }

    private Page<MetadataTransformerItemDto> getMetaByJsNode(String nodeId, Page<MetadataTransformerItemDto> result, DatabaseNode sourceNode, DatabaseNode targetNode, List<String> tableNames, List<String> currentTableList, DataSourceConnectionDto targetDataSource, List<Node<?>> predecessors) {
        // table rename
        LinkedList<TableRenameProcessNode> tableRenameProcessNodes = predecessors.stream()
                .filter(node -> node instanceof TableRenameProcessNode)
                .map(node -> (TableRenameProcessNode) node)
                .collect(Collectors.toCollection(LinkedList::new));
        Map<String, TableRenameTableInfo> tableNameMapping = null;
        if (CollectionUtils.isNotEmpty(tableRenameProcessNodes)) {
            tableNameMapping = tableRenameProcessNodes.getLast().originalMap();
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
                qualifiedNames.add(MetaDataBuilderUtils.generateQualifiedName(metaType, targetDataSource, tempName));
            } else {
                qualifiedNames.add(MetaDataBuilderUtils.generateQualifiedName(MetaType.processor_node.name(), nodeId, tempName));
            }
        }

        List<MetadataInstancesDto> instances = metadataInstancesService.findByQualifiedNameList(qualifiedNames);
        if (CollectionUtils.isNotEmpty(instances)) {
            List<MetadataTransformerItemDto> data = Lists.newArrayList();
            for (MetadataInstancesDto instance : instances) {
                MetadataTransformerItemDto item = new MetadataTransformerItemDto();
                item.setSourceObjectName(instance.getOriginalName());
                item.setPreviousTableName(instance.getOriginalName());
                item.setSinkObjectName(instance.getName());
                item.setSinkQulifiedName(instance.getQualifiedName());

                List<FieldsMapping> fieldsMapping = Lists.newArrayList();
                if (CollectionUtils.isNotEmpty(instance.getFields())) {
                    for (Field field : instance.getFields()) {
                        String defaultValue = Objects.isNull(field.getDefaultValue()) ? "" : field.getDefaultValue().toString();
                        int primaryKey = Objects.isNull(field.getPrimaryKeyPosition()) ? 0 : field.getPrimaryKeyPosition();
                        FieldsMapping mapping = new FieldsMapping(field.getFieldName(), field.getOriginalFieldName(),
                                field.getDataType(), "auto", defaultValue, true, "system", primaryKey);
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
    private Page<MetadataTransformerItemDto> getMetadataTransformerItemDtoPage(UserDetail userDetail, Page<MetadataTransformerItemDto> result, DatabaseNode sourceNode, DatabaseNode targetNode, List<String> tableNames, List<String> currentTableList, DataSourceConnectionDto targetDataSource, List<Node<?>> predecessors, Node<?> currentNode) {
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
        List<MetadataInstancesDto> list = metadataInstancesService.findBySourceIdAndTableNameList(sourceNode.getConnectionId(),
                currentTableList, userDetail);
        if (CollectionUtils.isNotEmpty(list)) {
            metaMap = list.stream().map(meta -> {
                // source & target not same database type
                if (currentNode instanceof DatabaseNode && !sourceDataSource.getDatabase_type().equals(targetDataSource.getDatabase_type())) {
                    Schema schema = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(meta), Schema.class);
                    return processFieldToDB(schema, meta, targetDataSource, userDetail);
                } else {
                    return meta;
                }
            }).collect(Collectors.toMap(MetadataInstancesDto::getOriginalName, Function.identity()));
        }

        List<MetadataTransformerItemDto> data = Lists.newArrayList();
        for (String tableName : currentTableList) {
            MetadataTransformerItemDto item = new MetadataTransformerItemDto();
            item.setSourceObjectName(tableName);
            String sinkTableName = tableName;
            String previousTableName = tableName;
            if (Objects.nonNull(tableNameMapping) && !tableNameMapping.isEmpty() && Objects.nonNull(tableNameMapping.get(tableName))) {
                sinkTableName = tableNameMapping.get(tableName).getCurrentTableName();
                previousTableName = tableNameMapping.get(tableName).getPreviousTableName();
            }
            item.setSinkObjectName(sinkTableName);

            List<FieldsMapping> fieldsMapping = Lists.newArrayList();
            // set qualifiedName
            String sinkQualifiedName = null;
            if (Objects.nonNull(targetDataSource)) {
                String metaType = "mongodb".equals(targetDataSource.getDatabase_type()) ? "collection" : "table";
                sinkQualifiedName = MetaDataBuilderUtils.generateQualifiedName(metaType, targetDataSource, tableName);
            }
            String metaType = "mongodb".equals(sourceDataSource.getDatabase_type()) ? "collection" : "table";
            String sourceQualifiedName = MetaDataBuilderUtils.generateQualifiedName(metaType, sourceDataSource, tableName);

            if (CollectionUtils.isEmpty(metaMap.get(tableName).getFields())) {
                continue;
            }
            List<Field> fields = metaMap.get(tableName).getFields().stream().filter(t -> !t.isDeleted()).collect(Collectors.toList());

            // TableRenameProcessNode not need fields
            if (!(currentNode instanceof TableRenameProcessNode)) {
                Map<String, FieldInfo> fieldInfoMap = null;
                if (Objects.nonNull(tableFieldMap) && !tableFieldMap.isEmpty() && tableFieldMap.containsKey(tableName)) {
                    fieldInfoMap = tableFieldMap.get(tableName).stream()
                            .filter(f -> Objects.nonNull(f.getSourceFieldName()))
                            .collect(Collectors.toMap(FieldInfo::getSourceFieldName, Function.identity()));
                }
                Map<String, FieldInfo> finalFieldInfoMap = fieldInfoMap;
                for (Field field : fields) {
                    String defaultValue = Objects.isNull(field.getDefaultValue()) ? "" : field.getDefaultValue().toString();
                    int primaryKey = Objects.isNull(field.getPrimaryKeyPosition()) ? 0 : field.getPrimaryKeyPosition();
                    String fieldName = field.getOriginalFieldName();
                    FieldsMapping mapping = new FieldsMapping(fieldName, fieldName, field.getDataType(),
                            "auto", defaultValue, true, "system", primaryKey);

                    if (Objects.nonNull(finalFieldInfoMap) && finalFieldInfoMap.containsKey(fieldName)) {
                        FieldInfo fieldInfo = finalFieldInfoMap.get(fieldName);

                        if (!(currentNode instanceof MigrateFieldRenameProcessorNode) && !fieldInfo.getIsShow()) {
                            continue;
                        }

                        mapping.setTargetFieldName(fieldInfo.getTargetFieldName());
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

    @Override
    public void testRunJsNode(TestRunDto dto, UserDetail userDetail) {
        String taskId = dto.getTaskId();
        String nodeId = dto.getJsNodeId();
        String tableName = dto.getTableName();
        Integer rows = dto.getRows();
        String script = dto.getScript();
        Long version = dto.getVersion();

        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));

        DAG dtoDag = taskDto.getDag();
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
        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(edges)) {
            Edge edge = new Edge(nodeId, target.getId());
            edges.add(edge);
        }

        Objects.requireNonNull(build).setNodes(new LinkedList<Node>(){{addAll(nodes);}});
        build.setEdges(edges);

        DAG temp = DAG.build(build);

        taskDto.setDag(null);
        taskDto.setSyncType(TaskDto.SYNC_TYPE_TEST_RUN);
        SubTaskDto subTaskDto = new SubTaskDto();
        subTaskDto.setStatus(SubTaskDto.STATUS_WAIT_RUN);
        subTaskDto.setParentTask(taskDto);
        subTaskDto.setDag(temp);
        subTaskDto.setParentId(taskDto.getId());
        subTaskDto.setId(new ObjectId());
        subTaskDto.setName(taskDto.getName() + "(100)");
        subTaskDto.setVersion(version);

        List<Worker> workers = workerService.findAvailableAgentByAccessNode(userDetail, taskDto.getAccessNodeProcessIdList());
        if (CollectionUtils.isEmpty(workers)) {
            throw new BizException("no agent");
        }

        MessageQueueDto queueDto = new MessageQueueDto();
        queueDto.setReceiver(workers.get(0).getProcessId());
        queueDto.setData(subTaskDto);
        queueDto.setType(TaskDto.SYNC_TYPE_TEST_RUN);
        messageQueueService.sendMessage(queueDto);
    }

    @Override
    public void saveResult(JsResultDto jsResultDto) {
        if (Objects.nonNull(jsResultDto)) {
            StringJoiner joiner = new StringJoiner(":", jsResultDto.getTaskId(), jsResultDto.getVersion().toString());
            CacheUtils.put(joiner.toString(),  jsResultDto);
        }
    }

    @Override
    public ResponseMessage<JsResultVo> getRun(String taskId, String jsNodeId, Long version) {
        ResponseMessage<JsResultVo> res = new ResponseMessage<>();
        JsResultVo result = new JsResultVo();
        StringJoiner joiner = new StringJoiner(":", taskId, version.toString());
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
                res.setMessage("SystemError");
            });
        } else {
            result.setOver(false);
            res.setData(result);
        }
        return res;
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
        String finalDbVersion = dbVersion;
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
                PdkSchemaConvert.tableFieldTypesGenerator.autoFill(updateFieldMap, DefaultExpressionMatchingMap.map(expression));

                updateFieldMap.forEach((k, v) -> {
                    tapTable.getNameFieldMap().replace(k, v);
                });
            }
        }

        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();

        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create().withTapTypeDataTypeMap(tapMap));
        TapResult<LinkedHashMap<String, TapField>> convert = PdkSchemaConvert.targetTypesGenerator.convert(nameFieldMap
                , DefaultExpressionMatchingMap.map(expression), codecsFilterManager);
        LinkedHashMap<String, TapField> data = convert.getData();

        data.forEach((k, v) -> {
            TapField tapField = nameFieldMap.get(k);
            BeanUtils.copyProperties(v, tapField);
        });
        tapTable.setNameFieldMap(nameFieldMap);



        metadataInstancesDto = PdkSchemaConvert.fromPdk(tapTable);


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
