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
    protected static final String[] METADATA_INCLUDE_FIELDS_CUSTOM  = new String[]{"_id", "sourceType", "original_name", "ancestorsName","fields.tapType","fields.dataType","fields.fieldName","fields.originalFieldName","fields.primaryKey","fields.columnPosition","custom_properties","nodeId","name"};

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
        Criteria virtualCriteria = new Criteria("sourceType").is(SourceTypeEnum.VIRTUAL.name());
        Query query = Query.query(new Criteria().andOperator(baseCriteria, virtualCriteria));
        query.fields().include(metadataIncludeFields());
        LineageMetadataInstance lineageMetadataInstance = getMetadata(query);
        if (null == lineageMetadataInstance) {
            Criteria sourceCriteria = new Criteria("sourceType").is(SourceTypeEnum.SOURCE.name());
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
            Node nodeInTask = findNodeInTask(task, connectionId, table);
            if (null == nodeInTask) {
                continue;
            }
            if (StringUtils.isBlank(nodeInTask.getId())) {
                continue;
            }
            com.tapdata.tm.commons.dag.DAG dag = task.getDag();
            if (null == dag || CollectionUtils.isEmpty(dag.getEdges())) {
                continue;
            }
            boolean hasIncoming = dag.getEdges().stream()
                    .anyMatch(e -> null != e && nodeInTask.getId().equals(e.getTarget()));
            if (!hasIncoming) {
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

    private static Criteria buildTaskCriteria(String connectionId, String table) {
        Criteria syncTaskCriteria = new Criteria("dag.nodes.connectionId").is(connectionId).and("dag.nodes.tableName").is(table);
        Criteria migrateSrcCriteria = new Criteria("dag.nodes.tableNames").is(table);
        Criteria migrateTgtCriteria = new Criteria("dag.nodes.syncObjects.objectNames").is(table);
        Criteria migrateCriteria = new Criteria("dag.nodes.connectionId").is(connectionId)
                .andOperator(new Criteria().orOperator(migrateSrcCriteria, migrateTgtCriteria));
        Criteria notDeleteCriteria = new Criteria("is_deleted").is(false);
        return new Criteria().andOperator(
                notDeleteCriteria,
                new Criteria().orOperator(syncTaskCriteria, migrateCriteria)
        );
    }

    private Node findNodeInTask(TaskEntity task, String connectionId, String table) {
        if (null == task || null == task.getDag()) {
            return null;
        }
        List<Node> nodes = task.getDag().getNodes();
        if (CollectionUtils.isEmpty(nodes)) {
            return null;
        }
        return nodes.stream().filter(node -> {
            if (node instanceof TableNode) {
                TableNode tableNode = (TableNode) node;
                return connectionId.equals(tableNode.getConnectionId()) && table.equals(tableNode.getTableName());
            } else if (node instanceof DatabaseNode) {
                DatabaseNode databaseNode = (DatabaseNode) node;
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
