package com.tapdata.tm.lineage.analyzer.impl;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.lineage.analyzer.AnalyzeLayer;
import com.tapdata.tm.lineage.analyzer.entity.LineageMetadataInstance;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.vo.SourceTypeEnum;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.task.entity.TaskEntity;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/20 16:58 Create
 * @description
 */
@Service("tableAnalyzerCustom")
public class TableAnalyzerCustom extends TableAnalyzerV1 {
    public static final String SOURCE_TYPE = "sourceType";
    protected static final String[] METADATA_INCLUDE_FIELDS_CUSTOM = new String[]{"_id", SOURCE_TYPE, "original_name", "ancestorsName", "fields.tapType", "fields.dataType", "fields.fieldName", "fields.originalFieldName", "fields.primaryKey", "fields.columnPosition", "custom_properties", "nodeId", "name"};

    @Override
    protected String[] metadataIncludeFields() {
        return METADATA_INCLUDE_FIELDS_CUSTOM;
    }

    @Override
    protected LineageMetadataInstance getMetadata(String connectionId, String tableName, String nodeId) {
        Criteria baseCriteria = new Criteria("source._id").is(connectionId)
                .and("name").is(tableName);
        if (null != nodeId) {
            baseCriteria.and("nodeId").is(nodeId);
        }
        Criteria virtualCriteria = new Criteria("").is(SourceTypeEnum.VIRTUAL.name());
        Query query = Query.query(new Criteria().andOperator(baseCriteria, virtualCriteria));
        query.fields().include(metadataIncludeFields());
        LineageMetadataInstance lineageMetadataInstance = getMetadata(query);
        if (null == lineageMetadataInstance) {
            Criteria sourceCriteria = new Criteria(SOURCE_TYPE).is(SourceTypeEnum.SOURCE.name());
            query = Query.query(new Criteria().andOperator(baseCriteria, sourceCriteria));
            query.fields().include(metadataIncludeFields());
            lineageMetadataInstance = getMetadata(query);
        }
        return lineageMetadataInstance;
    }

    @Override
    protected LineageMetadataInstance getMetadata(Query query) {
        List<MetadataInstancesEntity> metadataInstancesEntities = metadataInstancesRepository.findAll(query);
        MetadataInstancesEntity metadataInstancesEntity = metadataInstancesEntities.size() == 1 ? metadataInstancesEntities.get(0) : metadataInstancesEntities.stream()
                .filter(Objects::nonNull)
                .filter(e -> !e.getName().equals(e.getAncestorsName()))
                .findFirst()
                .orElse(null);
        if (null == metadataInstancesEntity || null == metadataInstancesEntity.getId()) {
            return null;
        }
        LineageMetadataInstance lineageMetadataInstance = new LineageMetadataInstance();
        lineageMetadataInstance.setNodeId(metadataInstancesEntity.getNodeId());
        lineageMetadataInstance.setFields(metadataInstancesEntity.getFields());
        lineageMetadataInstance.setCustomProperties(metadataInstancesEntity.getCustomProperties());
        lineageMetadataInstance.setId(metadataInstancesEntity.getId().toHexString());
        lineageMetadataInstance.setSourceType(metadataInstancesEntity.getSourceType());
        return lineageMetadataInstance;
    }

    @Override
    protected List<ModulesEntity> findModules(String connectionId, String table) {
        return new ArrayList<>();
    }

    @Override
    protected String getNodeIdIfNeedPreNodeId(AnalyzeLayer analyzeLayer, Node node, Node graphNode) {
        String graphNodeId = null == graphNode ? null : graphNode.getId();
        if (StringUtils.isBlank(graphNodeId)) {
            String upstreamNodeId = findUpstreamTaskNodeId(analyzeLayer.getConnectionId(), analyzeLayer.getTable());
            if (StringUtils.isNotBlank(upstreamNodeId)) {
                graphNodeId = upstreamNodeId;
            } else if (StringUtils.isNotBlank(node.getId())) {
                graphNodeId = node.getId();
            }
        }
        return graphNodeId;
    }

    private String findUpstreamTaskNodeId(String connectionId, String table) {
        if (StringUtils.isBlank(connectionId) || StringUtils.isBlank(table)) {
            return null;
        }
        Criteria taskCriteria = buildTaskCriteria(connectionId, table);
        Query query = Query.query(taskCriteria);
        query.fields().include(taskIncludeFields());
        List<TaskEntity> tasks = taskRepository.findAll(query);
        if (CollectionUtils.isEmpty(tasks)) {
            return null;
        }
        for (TaskEntity task : tasks) {
            Node<?> nodeInTask = findNodeOfTask(task, connectionId, table);
            com.tapdata.tm.commons.dag.DAG dag = findDag(nodeInTask, task);
            if (null == dag) {
                continue;
            }
            boolean hasOutgoing = dag.getEdges().stream()
                    .anyMatch(e -> null != e && nodeInTask.getId().equals(e.getSource()));
            if (!hasOutgoing) {
                return nodeInTask.getId();
            }
        }
        return null;
    }

    private com.tapdata.tm.commons.dag.DAG findDag(Node<?> nodeInTask, TaskEntity task) {
        if (null == nodeInTask) {
            return null;
        }
        if (StringUtils.isBlank(nodeInTask.getId())) {
            return null;
        }
        com.tapdata.tm.commons.dag.DAG dag = task.getDag();
        if (null == dag || CollectionUtils.isEmpty(dag.getEdges())) {
            return null;
        }
        boolean hasIncoming = dag.getEdges().stream()
                .anyMatch(e -> null != e && nodeInTask.getId().equals(e.getTarget()));
        if (!hasIncoming) {
            return null;
        }
        return dag;
    }

    private Node<?> findNodeOfTask(TaskEntity task, String connectionId, String table) {
        if (null == task || null == task.getDag()) {
            return null;
        }
        List<Node> nodes = task.getDag().getNodes();
        if (CollectionUtils.isEmpty(nodes)) {
            return null;
        }
        return nodes.stream().filter(node -> {
            if (node instanceof TableNode tableNode) {
                return connectionId.equals(tableNode.getConnectionId()) && table.equals(tableNode.getTableName());
            } else if (node instanceof DatabaseNode databaseNode) {
                if (!connectionId.equals(databaseNode.getConnectionId())) {
                    return false;
                }
                List<String> tableNames = databaseNode.getTableNames();
                if (CollectionUtils.isNotEmpty(tableNames)) {
                    return tableNames.contains(table);
                }
                List<SyncObjects> syncObjects = databaseNode.getSyncObjects();
                if (CollectionUtils.isNotEmpty(syncObjects)) {
                    SyncObjects objects = syncObjects.stream().filter(so -> CollectionUtils.isNotEmpty(so.getObjectNames())).findFirst().orElse(null);
                    if (null != objects) {
                        return objects.getObjectNames().contains(table);
                    }
                }
                return false;
            } else {
                return false;
            }
        }).findFirst().orElse(null);
    }
}
