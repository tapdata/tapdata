package com.tapdata.tm.task.service.impl;

import com.google.common.collect.Maps;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.FieldsMapping;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.MetadataTransformerItemDto;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.service.TaskNodeService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskNodeServiceImpl implements TaskNodeService {

    private TaskService taskService;
    private MetadataInstancesService metadataInstancesService;
    private DataSourceService dataSourceService;

    @Override
    public Page<MetadataTransformerItemDto> getNodeTableInfo(String taskId, String nodeId, String searchTableName,
                                                             Integer page, Integer pageSize, UserDetail userDetail) {
        Page<MetadataTransformerItemDto> result = new Page<>();

        DAG dag = taskService.findById(MongoUtils.toObjectId(taskId)).getDag();

        LinkedList<DatabaseNode> databaseNodes = dag.getNodes().stream()
                .filter(node -> node instanceof DatabaseNode)
                .map(node -> (DatabaseNode) node)
                .collect(Collectors.toCollection(LinkedList::new));
        if (CollectionUtils.isEmpty(databaseNodes)) {
            return result;
        }

        DatabaseNode sourceNode = dag.getSourceNode().getFirst();
        DatabaseNode targetNode = org.apache.commons.collections4.CollectionUtils.isNotEmpty(dag.getTargetNode()) ? dag.getTargetNode().getLast() : null;
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

        List<Node> predecessors = dag.getPreNodes(nodeId);
        Node currentNode = dag.getNode(nodeId);
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
                        .collect(Collectors.toMap(TableFieldInfo::getOriginTableName, TableFieldInfo::getFields, (e1,e2)->e1));
            }
        }

        Map<String, MetadataInstancesEntity> metaMap = Maps.newHashMap();
        List<MetadataInstancesEntity> list = metadataInstancesService.findEntityBySourceIdAndTableNameList(sourceNode.getConnectionId(),
                currentTableList, userDetail);
        if (CollectionUtils.isNotEmpty(list)) {
            metaMap = list.stream().collect(Collectors.toMap(MetadataInstancesEntity::getOriginalName, Function.identity()));
        }

        DataSourceConnectionDto sourceDataSource = dataSourceService.findById(MongoUtils.toObjectId(sourceNode.getConnectionId()));

        DataSourceConnectionDto targetDataSource = null;
        if (targetNode != null) {
            targetDataSource = dataSourceService.findById(MongoUtils.toObjectId(targetNode.getConnectionId()));
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
                sinkQualifiedName = MetaDataBuilderUtils.generateQualifiedName(metaType, targetDataSource, tableName, taskId);
            }
            String metaType = "mongodb".equals(sourceDataSource.getDatabase_type()) ? "collection" : "table";
            String sourceQualifiedName = MetaDataBuilderUtils.generateQualifiedName(metaType, sourceDataSource, tableName, taskId);

            List<Field> fields = metaMap.get(tableName).getFields();

            // TableRenameProcessNode not need fields
            if (!(currentNode instanceof TableRenameProcessNode)) {
                Map<String, FieldInfo> fieldInfoMap = null;
                if (Objects.nonNull(tableFieldMap) && !tableFieldMap.isEmpty() && tableFieldMap.containsKey(tableName)) {
                    fieldInfoMap = tableFieldMap.get(tableName).stream()
                            .filter(f -> Objects.nonNull(f.getSourceFieldName()))
                            .collect(Collectors.toMap(FieldInfo::getSourceFieldName, Function.identity()));
                }
                Map<String, FieldInfo> finalFieldInfoMap = fieldInfoMap;
                for (int i = 0; i < fields.size(); i++) {
                    Field field = fields.get(i);
                    String defaultValue = Objects.isNull(field.getDefaultValue()) ? "" : field.getDefaultValue().toString();

                    String fieldName = field.getOriginalFieldName();
                    FieldsMapping mapping = new FieldsMapping(fieldName, fieldName, field.getDataType(), "auto", defaultValue, true, "system");

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
}
