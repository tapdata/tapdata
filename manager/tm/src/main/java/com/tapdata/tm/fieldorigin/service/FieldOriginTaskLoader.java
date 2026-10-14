package com.tapdata.tm.fieldorigin.service;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.fieldorigin.builder.NodeSchema;
import com.tapdata.tm.fieldorigin.builder.NodeSchemaProvider;
import com.tapdata.tm.fieldorigin.builder.OriginGraphNode;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Converts a task into builder input: the task dag and the derived models saved in MetadataInstances.
 */
@Component
public class FieldOriginTaskLoader {
    private final MongoTemplate mongoTemplate;

    public FieldOriginTaskLoader(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public List<OriginGraphNode> graph(TaskDto task) {
        DAG dag = null == task ? null : task.getDag();
        if (null == dag || CollectionUtils.isEmpty(dag.getNodes())) {
            return new ArrayList<>();
        }
        List<OriginGraphNode> graph = new ArrayList<>();
        for (Node node : dag.getNodes()) {
            if (null == node || StringUtils.isBlank(node.getId())) {
                continue;
            }
            OriginGraphNode originNode;
            if (node instanceof DataParentNode<?> dataNode && node.isDataNode()) {
                originNode = OriginGraphNode.data(node.getId(), node.getType(), dataNode.getConnectionId());
            } else {
                originNode = OriginGraphNode.processor(node.getId(), node.getType());
                if (node instanceof MergeTableNode mergeTableNode) {
                    originNode.setMergeProperties(mergeTableNode.getMergeProperties());
                }
            }
            for (Object predecessor : CollectionUtils.emptyIfNull(dag.predecessors(node.getId()))) {
                if (predecessor instanceof Node<?> p && StringUtils.isNotBlank(p.getId())) {
                    originNode.getPredecessors().add(p.getId());
                }
            }
            graph.add(originNode);
        }
        return graph;
    }

    public NodeSchemaProvider schemaProvider(String taskId) {
        Map<String, List<NodeSchema>> cache = new HashMap<>();
        return nodeId -> cache.computeIfAbsent(nodeId, id -> load(taskId, id));
    }

    List<NodeSchema> load(String taskId, String nodeId) {
        Query query = Query.query(Criteria.where("taskId").is(taskId).and("nodeId").is(nodeId).and("is_deleted").ne(true));
        query.fields().include("fields", "original_name", "nodeId");
        return mongoTemplate.find(query, MetadataInstancesEntity.class).stream()
                .filter(Objects::nonNull)
                .map(m -> new NodeSchema(nodeId, m.getOriginalName(), m.getFields()))
                .toList();
    }
}
