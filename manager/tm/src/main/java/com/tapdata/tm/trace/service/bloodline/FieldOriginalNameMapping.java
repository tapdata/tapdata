package com.tapdata.tm.trace.service.bloodline;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.TableIndexColumn;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.analyzer.entity.LineageMetadataInstance;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTask;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.trace.dto.boodline.FieldNameMapping;
import com.tapdata.tm.trace.dto.boodline.TableProperties;
import com.tapdata.tm.utils.MongoUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/28 12:14 Create
 * @description
 */
@Service
public class FieldOriginalNameMapping {
    private static final String TASK_ID = "taskId";
    private static final String NODE_ID = "nodeId";
    private static final String FIELDS = "fields";
    private static final String INDICES = "indices";
    private static final String IS_DELETED = "is_deleted";
    @Resource
    MetadataInstancesRepository metadataInstancesRepository;

    public Map<String, Map<String, String>> groupFieldOriginalNameMappingByNodeId(List<Node> nodes) {
        Map<String, Map<String, String>> result = new HashMap<>();
        if (CollectionUtils.isEmpty(nodes)) {
            return result;
        }
        for (Node<?> node : nodes) {
            if (null == node || StringUtils.isBlank(node.getId())) {
                continue;
            }
            Map<String, String> fieldNameToOriginalName = new HashMap<>();
            if (node instanceof LineageTableNode lineageTableNode) {
                LineageMetadataInstance metadata = lineageTableNode.getMetadata();
                List<Field> fields = null == metadata ? null : metadata.getFields();
                if (CollectionUtils.isNotEmpty(fields)) {
                    for (Field field : fields) {
                        if (null == field || field.isDeleted() || StringUtils.isBlank(field.getFieldName())) {
                            continue;
                        }
                        fieldNameToOriginalName.putIfAbsent(field.getFieldName(), StringUtils.defaultString(field.getOriginalFieldName()));
                    }
                }
            }
            result.put(node.getId(), fieldNameToOriginalName);
        }
        for (Node<?> node : nodes) {
            if (null == node || StringUtils.isBlank(node.getId())) {
                continue;
            }
            result.putIfAbsent(node.getId(), new HashMap<>());
        }
        return result;
    }

    public void findUpdateConditionField(Dag dag, Map<String, DAG> taskDagMap, Map<String, Map<String, String>> fieldNameMapping) {
        //把Dag中所有节点都补充节点当前表的更新条件字段
        //作为源表时：更新条件字段来源于此表所在血缘关系中上游任务作为目标时的节点配置上，如果此表无上游任务则之间取此表的主键字段列表或者索引字段列表
        //作为目标表时：直接从任务配置中获取节点配置的更新条件字段列表
        //获取到的字段列表后需要找到最终目标表与之对应的字段名 --> CurrentFieldNameMapping
        //需要将结果补充到dag.nodes对应的表节点的attr.$taskId下的com.tapdata.tm.trace.service.bloodline.BloodlineFinder.TableProperties.updateConditionField中的updateConditionField属性上

        if (null == dag || CollectionUtils.isEmpty(dag.getNodes()) || MapUtils.isEmpty(taskDagMap)) {
            return;
        }
        Map<String, Map<String, String>> fieldNameMappingByNodeId = MapUtils.isEmpty(fieldNameMapping) ? new HashMap<>() : fieldNameMapping;
        LineageTableNode finalTarget = findFinalTargetLineageTableNode(dag);
        String finalTargetNodeId = null == finalTarget ? null : finalTarget.getId();
        Map<String, List<String>> targetCandidatesByOriginName = new HashMap<>();
        if (StringUtils.isNotBlank(finalTargetNodeId)) {
            Map<String, String> targetFieldToOrigin = fieldNameMappingByNodeId.getOrDefault(finalTargetNodeId, new HashMap<>());
            if (MapUtils.isNotEmpty(targetFieldToOrigin)) {
                targetFieldToOrigin.forEach((targetFieldName, originName) -> {
                    if (StringUtils.isBlank(targetFieldName) || StringUtils.isBlank(originName)) {
                        return;
                    }
                    targetCandidatesByOriginName.computeIfAbsent(originName, k -> new ArrayList<>()).add(targetFieldName);
                });
            }
        }
        eachDagNode(dag, taskDagMap);
    }

    public String getOriginFieldName(String taskId, String nodeId, String tableName, String targetName, Map<String, Map<String, Field>> fieldsByNodeIdCache) {
        Field field = getField(taskId, nodeId, tableName, targetName, fieldsByNodeIdCache);
        if (null != field && StringUtils.isNotBlank(field.getOriginalFieldName())) {
            return field.getOriginalFieldName();
        }
        return targetName;
    }

    public void addFieldNameMapping(List<FieldNameMapping> list, String originName, String targetName) {
        if (null == list || StringUtils.isBlank(originName) || StringUtils.isBlank(targetName)) {
            return;
        }
        for (FieldNameMapping mapping : list) {
            if (null == mapping) {
                continue;
            }
            if (originName.equals(mapping.getOriginName()) && targetName.equals(mapping.getTargetName())) {
                return;
            }
        }
        FieldNameMapping mapping = new FieldNameMapping();
        mapping.setOriginName(originName);
        mapping.setTargetName(targetName);
        list.add(mapping);
    }

    protected Field getField(String taskId, String nodeId, String tableName, String fieldName, Map<String, Map<String, Field>> fieldsByNodeIdCache) {
        if (StringUtils.isBlank(taskId) || StringUtils.isBlank(nodeId) || StringUtils.isBlank(fieldName)) {
            return null;
        }
        Map<String, Field> fieldMap = getFieldMapForNode(taskId, nodeId, tableName, fieldsByNodeIdCache);
        return fieldMap.get(fieldName);
    }

    protected Map<String, Field> getFieldMapForNode(String taskId, String nodeId, String tableName, Map<String, Map<String, Field>> fieldsByNodeIdCache) {
        if (StringUtils.isBlank(taskId) || StringUtils.isBlank(nodeId)) {
            return new HashMap<>();
        }
        String cacheKey = nodeId + "::" + StringUtils.defaultString(tableName);
        return fieldsByNodeIdCache.computeIfAbsent(cacheKey, k -> {
            Criteria criteria = Criteria.where(TASK_ID).is(taskId)
                    .and(NODE_ID).is(nodeId)
                    .and(IS_DELETED).ne(true);
            if (StringUtils.isNotBlank(tableName)) {
                criteria = criteria.and("original_name").is(tableName);
            }
            Query query = Query.query(criteria);
            query.fields().include(FIELDS);
            MetadataInstancesEntity entity = metadataInstancesRepository.findOne(query).orElse(null);
            List<Field> fields = null == entity ? null : entity.getFields();
            if (CollectionUtils.isEmpty(fields)) {
                return new HashMap<>();
            }
            return fields.stream()
                    .filter(Objects::nonNull)
                    .filter(f -> StringUtils.isNotBlank(f.getFieldName()))
                    .collect(Collectors.toMap(Field::getFieldName, f -> f, (f1, f2) -> f1));
        });
    }

    void eachDagNode(Dag dag, Map<String, DAG> taskDagMap) {
        for (Node<?> n : dag.getNodes()) {
            if (!(n instanceof LineageTableNode lineageTableNode) || StringUtils.isBlank(lineageTableNode.getId()) || MapUtils.isEmpty(lineageTableNode.getTasks())) {
                continue;
            }
            Map<String, LineageTask> tasks = lineageTableNode.getTasks();
            Map<String, Object> attrs = attrs(lineageTableNode);
            tasks.values().stream()
                    .filter(Objects::nonNull)
                    .filter(lineageTask -> StringUtils.isNotBlank(lineageTask.getId()))
                    .forEach(lineageTask -> {
                        String taskId = lineageTask.getId();
                        DAG taskDag = taskDagMap.get(taskId);
                        if (null == taskDag) {
                            return;
                        }
                        eachLineageTask(lineageTask, lineageTableNode, attrs, taskDag, taskDagMap);
                    });
        }
    }

    Map<String, Object> attrs(LineageTableNode lineageTableNode) {
        Map<String, Object> attrs = lineageTableNode.getAttrs();
        if (null == attrs) {
            attrs = new HashMap<>();
            lineageTableNode.setAttrs(attrs);
        }
        return attrs;
    }

    void eachLineageTask(LineageTask lineageTask, LineageTableNode lineageTableNode, Map<String, Object> attrs, DAG taskDag, Map<String, DAG> taskDagMap) {
        String taskId = lineageTask.getId();
        String effectiveTaskId = effectiveTaskId(taskDag, taskId);

        Node<?> lineageTaskNode = lineageTask.getTaskNode();
        String taskNodeId = null == lineageTaskNode ? null : lineageTaskNode.getId();
        boolean targetPos = isTaskNodeTargetPos(lineageTaskNode);

        List<String> updateFields;
        if (targetPos) {
            TableNode tableNodeInTaskDag = findTableNodeInTaskDag(taskDag, taskNodeId, lineageTableNode.getConnectionId(), lineageTableNode.getTable());
            updateFields = null == tableNodeInTaskDag ? new ArrayList<>() : Optional.ofNullable(tableNodeInTaskDag.getUpdateConditionFields()).orElse(new ArrayList<>());
            if (CollectionUtils.isEmpty(updateFields) && null != tableNodeInTaskDag) {
                updateFields = getPrimaryOrUniqueKeyFieldsForNodeOrSource(effectiveTaskId, tableNodeInTaskDag);
            }
        } else {
            updateFields = tryGetUpdateFieldsFromUpstreamTarget(lineageTableNode, effectiveTaskId, taskDagMap);
            if (CollectionUtils.isEmpty(updateFields)) {
                TableNode tableNodeInTaskDag = findTableNodeInTaskDag(taskDag, taskNodeId, lineageTableNode.getConnectionId(), lineageTableNode.getTable());
                if (null != tableNodeInTaskDag) {
                    updateFields = getPrimaryOrUniqueKeyFieldsForNodeOrSource(effectiveTaskId, tableNodeInTaskDag);
                } else {
                    updateFields = getPrimaryOrUniqueKeyFieldsFromSource(lineageTableNode.getConnectionId(), lineageTableNode.getTable());
                }
            }
        }
        if (CollectionUtils.isEmpty(updateFields)) {
            return;
        }
        tableProperties(taskId, attrs, lineageTableNode, updateFields);
    }

    void tableProperties(String taskId, Map<String, Object> attrs, LineageTableNode lineageTableNode, List<String> updateFields) {
        TableProperties tableProperties;
        Object existing = attrs.get(taskId);
        if (existing instanceof TableProperties e) {
            tableProperties = e;
        } else {
            tableProperties = JoinStateSetter.newTableProperties(lineageTableNode.getId(), lineageTableNode.getId());
            attrs.put(taskId, tableProperties);
        }
        tableProperties.setUpdateConditionField(updateFields);
    }

    String effectiveTaskId(DAG taskDag, String taskId) {
        if (null == taskDag.getTaskId()) {
            ObjectId objectId = MongoUtils.toObjectId(taskId);
            if (null != objectId) {
                taskDag.setTaskId(objectId);
                return objectId.toHexString();
            }
        } else {
            return taskDag.getTaskId().toHexString();
        }
        return taskId;
    }

    private TableNode findTableNodeInTaskDag(DAG taskDag, String nodeId, String connectionId, String tableName) {
        if (null == taskDag) {
            return null;
        }
        if (StringUtils.isNotBlank(nodeId)) {
            Node<?> n = taskDag.getNode(nodeId);
            if (n instanceof TableNode tableNode) {
                return tableNode;
            }
        }
        if (StringUtils.isBlank(connectionId) || StringUtils.isBlank(tableName)) {
            return null;
        }
        for (Node<?> n : CollectionUtils.emptyIfNull(taskDag.getNodes())) {
            if (!(n instanceof TableNode)) {
                continue;
            }
            TableNode tn = (TableNode) n;
            if (connectionId.equals(tn.getConnectionId()) && tableName.equals(tn.getTableName())) {
                return tn;
            }
        }
        return null;
    }

    protected List<String> tryGetUpdateFieldsFromUpstreamTarget(LineageTableNode lineageTableNode, String currentTaskId, Map<String, DAG> taskDagMap) {
        if (null == lineageTableNode || MapUtils.isEmpty(lineageTableNode.getTasks())) {
            return new ArrayList<>();
        }
        for (LineageTask lineageTask : lineageTableNode.getTasks().values()) {
            if (null == lineageTask || StringUtils.isBlank(lineageTask.getId()) || lineageTask.getId().equals(currentTaskId)) {
                continue;
            }
            Node<?> taskNode = lineageTask.getTaskNode();
            Object pos = taskNode instanceof com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode iNode ? iNode.getTaskNodePos() : null;
            if (!StringUtils.equalsIgnoreCase(String.valueOf(pos), com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode.TASK_NODE_TARGET_POS)) {
                continue;
            }
            DAG upstreamDag = taskDagMap.get(lineageTask.getId());
            if (null == upstreamDag) {
                continue;
            }
            String upstreamTaskId = null == upstreamDag.getTaskId() ? lineageTask.getId() : upstreamDag.getTaskId().toHexString();
            TableNode upstreamTarget = upstreamDag.getTargets().stream()
                    .filter(TableNode.class::isInstance)
                    .map(TableNode.class::cast)
                    .findFirst()
                    .orElse(null);
            if (null == upstreamTarget) {
                continue;
            }
            List<String> configured = upstreamTarget.getUpdateConditionFields();
            if (CollectionUtils.isNotEmpty(configured)) {
                return configured;
            }
            return getPrimaryOrUniqueKeyFieldsForNodeOrSource(upstreamTaskId, upstreamTarget);
        }
        return new ArrayList<>();
    }

    public List<String> getPrimaryOrUniqueKeyFieldsForNodeOrSource(String taskId, TableNode tableNode) {
        if (null == tableNode) {
            return new ArrayList<>();
        }
        String nodeId = tableNode.getId();
        return getPrimaryOrUniqueKeyFieldsForNodeOrSource(taskId, nodeId, tableNode.getConnectionId(), tableNode.getTableName());
    }

    public List<String> getPrimaryOrUniqueKeyFieldsForNodeOrSource(String taskId, String nodeId, String connectionId, String tableName) {
        List<String> fields = getPrimaryOrUniqueKeyFields(taskId, nodeId);
        if (CollectionUtils.isNotEmpty(fields)) {
            return fields;
        }
        return getPrimaryOrUniqueKeyFieldsFromSource(connectionId, tableName);
    }

    protected List<String> getPrimaryOrUniqueKeyFields(String taskId, String nodeId) {
        if (StringUtils.isBlank(taskId) || StringUtils.isBlank(nodeId)) {
            return new ArrayList<>();
        }
        Query query = Query.query(Criteria.where(TASK_ID).is(taskId)
                .and(NODE_ID).is(nodeId)
                .and(IS_DELETED).ne(true));
        query.fields().include(FIELDS, INDICES);
        MetadataInstancesEntity entity = metadataInstancesRepository.findOne(query).orElse(null);
        if (null == entity) {
            return new ArrayList<>();
        }
        List<Field> fields = entity.getFields();
        List<String> primaryKeys = primaryKeys(fields);
        if (CollectionUtils.isNotEmpty(primaryKeys)) {
            return primaryKeys;
        }
        List<TableIndex> indices = Optional.ofNullable(entity.getIndices())
                .orElse(new ArrayList<>())
                .stream()
                .filter(Objects::nonNull)
                .filter(TableIndex::isUnique)
                .filter(e -> CollectionUtils.isNotEmpty(e.getColumns()))
                .toList();
        if (CollectionUtils.isEmpty(indices)) {
            return new ArrayList<>();
        }
        for (TableIndex index : indices) {
            List<String> uniqueKeyFields = new ArrayList<>();
            for (TableIndexColumn column : index.getColumns()) {
                if (null != column && StringUtils.isNotBlank(column.getColumnName())) {
                    uniqueKeyFields.add(column.getColumnName());
                }
            }
            if (CollectionUtils.isNotEmpty(uniqueKeyFields)) {
                return uniqueKeyFields;
            }
        }
        return new ArrayList<>();
    }

    protected List<String> getPrimaryOrUniqueKeyFieldsFromSource(String connectionId, String tableName) {
        if (StringUtils.isBlank(connectionId) || StringUtils.isBlank(tableName)) {
            return new ArrayList<>();
        }
        Criteria criteria = Criteria.where("source._id").is(connectionId)
                .and("original_name").is(tableName)
                .and("sourceType").is("SOURCE")
                .and(IS_DELETED).ne(true);
        Query query = Query.query(criteria);
        query.fields().include(FIELDS, INDICES);
        MetadataInstancesEntity entity = metadataInstancesRepository.findOne(query).orElse(null);
        if (null == entity) {
            return new ArrayList<>();
        }
        return extractPrimaryOrUniqueKeyFields(entity.getFields(), entity.getIndices());
    }

    List<String> primaryKeys(List<Field> fields) {
        List<String> primaryKeys = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(fields)) {
            for (Field field : fields) {
                if (null == field || field.isDeleted() || StringUtils.isBlank(field.getFieldName())) {
                    continue;
                }
                Integer pkPos = field.getPrimaryKeyPosition();
                if (Boolean.TRUE.equals(field.getPrimaryKey()) || (null != pkPos && pkPos > 0)) {
                    primaryKeys.add(field.getFieldName());
                }
            }
        }
        return primaryKeys;
    }

    protected List<String> extractPrimaryOrUniqueKeyFields(List<Field> fields, List<TableIndex> indices) {
        List<String> primaryKeys = primaryKeys(fields);
        if (CollectionUtils.isNotEmpty(primaryKeys)) {
            return primaryKeys;
        }
        if (CollectionUtils.isEmpty(indices)) {
            return new ArrayList<>();
        }
        for (TableIndex index : indices) {
            if (null == index || !index.isUnique() || CollectionUtils.isEmpty(index.getColumns())) {
                continue;
            }
            List<TableIndexColumn> columns = index.getColumns();
            List<String> uniqueKeyFields = new ArrayList<>();
            for (TableIndexColumn column : columns) {
                if (null != column && StringUtils.isNotBlank(column.getColumnName())) {
                    uniqueKeyFields.add(column.getColumnName());
                }
            }
            if (CollectionUtils.isNotEmpty(uniqueKeyFields)) {
                return uniqueKeyFields;
            }
        }
        return new ArrayList<>();
    }

    public boolean isTaskNodeTargetPos(Node<?> taskNode) {
        if (!(taskNode instanceof com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode)) {
            return false;
        }
        Object pos = ((com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode) taskNode).getTaskNodePos();
        return StringUtils.equalsIgnoreCase(String.valueOf(pos), com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode.TASK_NODE_TARGET_POS);
    }

    public LineageTableNode findFinalTargetLineageTableNode(Dag lineageDag) {
        if (null == lineageDag || CollectionUtils.isEmpty(lineageDag.getNodes())) {
            return null;
        }
        Map<String, LineageTableNode> tableNodeById = new HashMap<>();
        for (Node<?> n : lineageDag.getNodes()) {
            if (n instanceof LineageTableNode tableNode && StringUtils.isNotBlank(tableNode.getId())) {
                tableNodeById.put(tableNode.getId(), tableNode);
            }
        }
        if (tableNodeById.isEmpty()) {
            return null;
        }
        Set<String> sources = new HashSet<>();
        if (CollectionUtils.isNotEmpty(lineageDag.getEdges())) {
            for (Edge e : lineageDag.getEdges()) {
                if (null != e && StringUtils.isNotBlank(e.getSource())) {
                    sources.add(e.getSource());
                }
            }
        }
        List<LineageTableNode> sinkTableNodes = tableNodeById.values()
                .stream()
                .filter(n -> !sources.contains(n.getId()))
                .toList();
        if (CollectionUtils.isEmpty(sinkTableNodes)) {
            sinkTableNodes = new ArrayList<>(tableNodeById.values());
        }
        if (sinkTableNodes.size() == 1) {
            return sinkTableNodes.get(0);
        }
        List<LineageTableNode> nonSourceType = sinkTableNodes.stream()
                .filter(n -> null == n.getMetadata() || !"SOURCE".equalsIgnoreCase(n.getMetadata().getSourceType()))
                .toList();
        List<LineageTableNode> candidates = new ArrayList<>(CollectionUtils.isEmpty(nonSourceType) ? sinkTableNodes : nonSourceType);
        candidates.sort(Comparator.comparing(Node::getId, Comparator.nullsLast(String::compareTo)));
        return candidates.get(0);
    }
}
