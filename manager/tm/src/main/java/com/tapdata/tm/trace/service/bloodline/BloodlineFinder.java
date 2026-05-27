package com.tapdata.tm.trace.service.bloodline;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.TableIndexColumn;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.lineage.analyzer.AnalyzerService;
import com.tapdata.tm.lineage.analyzer.entity.LineageMetadataInstance;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTask;
import com.tapdata.tm.lineage.entity.LineageType;
import com.tapdata.tm.lineage.util.LineageTypeUtil;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.trace.dto.TargetWithLineageDto;
import com.tapdata.tm.trace.dto.TaskLineageDto;
import com.tapdata.tm.trace.param.TaskLineageParam;
import com.tapdata.tm.utils.MongoUtils;
import io.github.openlg.graphlib.Graph;
import io.tapdata.entity.simplify.TapSimplify;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/20 13:14 Create
 * @description node.attr.type: <$taskId, 'JOIN' ｜ 'MERGE' ｜ 'APPEND' ｜ 'OTHER'>
 * node.attr.joinKeys: <$taskId, ['xxx'...]>
 * node.attr.tablePk: <$taskId, ['yyy'...]>
 */
@Service
@Slf4j
public class BloodlineFinder {
    private static final String TASK_ID = "taskId";
    private static final String NODE_ID = "nodeId";
    private static final String FIELDS = "fields";
    private static final String INDICES = "indices";
    private static final String IS_DELETED = "is_deleted";
    @Resource(name = "tableAnalyzerCustom")
    AnalyzerService analyzerCustom;
    @Resource
    TaskService taskService;
    @Resource
    MetadataInstancesRepository metadataInstancesRepository;

    private static final String MAIN_TABLE = "mainTable";
    private static final String SUB_TABLE = "subTable";
    private static final String NODE_ATTR_TYPE_JOIN = "JOIN";
    private static final String NODE_ATTR_TYPE_MERGE = "MERGE";
    private static final String NODE_ATTR_TYPE_APPEND = "APPEND";
    private static final String NODE_ATTR_TYPE_OTHER = "OTHER";

    public TaskLineageDto findTaskLineage(TaskLineageParam param) {
        TaskLineageDto taskLineage = new TaskLineageDto(findLineage(param));
        Dag dag = taskLineage.getDag();
        Map<String, DAG> taskDagMap = findTaskDagMap(dag);
        Map<String, Map<String, String>> fieldNameMapping = groupFieldOriginalNameMappingByNodeId(dag.getNodes());
        markJoinState(dag, fieldNameMapping, taskDagMap);
        Map<String, List<FieldNameMapping>> updateConditionFieldList = UpdateConditionFieldLoader.getUpdateConditionFieldList(dag, taskDagMap, fieldNameMapping);
        taskLineage.setUpdateConditionFieldList(updateConditionFieldList);
        taskLineage.setFieldNameMapping(fieldNameMapping);
        Map<String, Map<String, String>> traceFilterFielMap = removeUselessFields(dag, param.getTraceFilterFieldNames(), fieldNameMapping);
        taskLineage.setTraceFilterFieldNameMapping(traceFilterFielMap);
        return taskLineage;
    }

    public TargetWithLineageDto findTaskLineageSimply(TaskLineageParam param) {
        TargetWithLineageDto taskLineage = new TargetWithLineageDto(findLineage(param));
        Dag dag = taskLineage.getDag();
        Map<String, DAG> taskDagMap = findTaskDagMap(dag);
        Map<String, Map<String, String>> fieldNameMapping = groupFieldOriginalNameMappingByNodeId(dag.getNodes());
        markJoinState(dag, fieldNameMapping, taskDagMap);
        List<String> targetTableUpdateFields = getTargetTableUpdateFields(dag, taskDagMap);
        taskLineage.setTargetTableUpdateFields(targetTableUpdateFields);
        findUpdateConditionField(dag, taskDagMap, fieldNameMapping);
        Map<String, Map<String, String>> traceFilterFielMap = removeUselessFields(dag, param.getTraceFilterFieldNames(), fieldNameMapping);
        taskLineage.setTraceFilterFieldNameMapping(traceFilterFielMap);
        return taskLineage;
    }

    public Dag findLineage(TaskLineageParam param) {
        LineageType lineageType = LineageTypeUtil.initLineageType(param.getType(), LineageType.UPSTREAM);
        try {
            Graph<Node, Edge> graph = analyzerCustom.analyzeTable(
                    param.getConnectionId(),
                    param.getTable(),
                    lineageType
            );
            if (null == graph) {
                graph = new Graph<>();
            }
            DAG dag = new DAG(graph);
            return dag.toDag();
        } catch (Exception e) {
            throw new BizException("data.trace.findDag.error", TapSimplify.toJson(param), e.getMessage());
        }
    }

    protected void findUpdateConditionField(Dag dag, Map<String, DAG> taskDagMap, Map<String, Map<String, String>> fieldNameMapping) {
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
            tableProperties = newTableProperties(lineageTableNode.getId(), lineageTableNode.getId());
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

    protected Map<String, Map<String, String>> removeUselessFields(Dag dag, List<String> traceFilterFieldNames, Map<String, Map<String, String>> fieldNameMapping) {
        if (null == dag || null == dag.getNodes()) {
            return new HashMap<>();
        }
        Map<String, Map<String, String>> traceFilterFielMap = removeUnTraceFilterFieldName(dag, traceFilterFieldNames, fieldNameMapping);
        dag.getNodes().forEach(this::removeUselessFields);
        return traceFilterFielMap;
    }

    protected Map<String, Map<String, String>> removeUnTraceFilterFieldName(Dag dag, List<String> traceFilterFieldNames, Map<String, Map<String, String>> fieldNameMapping) {
        if (CollectionUtils.isEmpty(traceFilterFieldNames)) {
            return new HashMap<>();
        }
        //traceFilterFieldNames时整个血缘图中最终目标节点的字段列表
        //利用fieldNameMapping找出traceFilterFieldNames对应在各节点中的字段名映射列表，如果没有就是空列表,这个字段名映射列表就是本方法的返回值
        //找出dag中所有node节点中不包含traceFilterFieldNames任何一个字段的节点，把这些节点从dag的node节点中移除，并把相应在edges中的属性也移除
        if (null == dag || CollectionUtils.isEmpty(dag.getNodes())) {
            return new HashMap<>();
        }

        Map<String, Map<String, String>> fieldNameMappingByNodeId = MapUtils.isEmpty(fieldNameMapping) ? new HashMap<>() : fieldNameMapping;
        LineageTableNode finalTarget = findFinalTargetLineageTableNode(dag);
        if (null == finalTarget || StringUtils.isBlank(finalTarget.getId())) {
            return new HashMap<>();
        }
        String targetNodeId = finalTarget.getId();

        Set<String> targetFields = traceFilterFieldNames.stream().filter(StringUtils::isNotBlank).collect(Collectors.toSet());
        if (targetFields.isEmpty()) {
            return new HashMap<>();
        }

        Map<String, String> targetNodeFieldToOrigin = fieldNameMappingByNodeId.getOrDefault(targetNodeId, new HashMap<>());
        Map<String, String> targetFieldToOriginName = new HashMap<>();
        for (String targetField : targetFields) {
            String originName = targetNodeFieldToOrigin.get(targetField);
            if (StringUtils.isBlank(originName)) {
                continue;
            }
            targetFieldToOriginName.put(targetField, originName);
        }

        Set<String> keptNodeIds = new HashSet<>();
        keptNodeIds.add(targetNodeId);
        Map<String, Map<String, String>> result = new HashMap<>();

        for (Node<?> node : dag.getNodes()) {
            eachAllNodes(node, targetNodeId, targetFields, keptNodeIds, fieldNameMappingByNodeId, result, targetFieldToOriginName);
        }

        List<Node> newNodes = dag.getNodes().stream()
                .filter(Objects::nonNull)
                .filter(n -> StringUtils.isNotBlank(n.getId()))
                .filter(n -> keptNodeIds.contains(n.getId()))
                .toList();
        dag.setNodes(newNodes);

        if (CollectionUtils.isNotEmpty(dag.getEdges())) {
            List<Edge> newEdges = dag.getEdges().stream()
                    .filter(Objects::nonNull)
                    .filter(e -> StringUtils.isNotBlank(e.getSource()) && StringUtils.isNotBlank(e.getTarget()))
                    .filter(e -> keptNodeIds.contains(e.getSource()) && keptNodeIds.contains(e.getTarget()))
                    .toList();
            dag.setEdges(newEdges);
        }

        return result;
    }

    void eachAllNodes(Node<?> n,
                      String targetNodeId,
                      Set<String> targetFields,
                      Set<String> keptNodeIds,
                      Map<String, Map<String, String>> fieldNameMappingByNodeId,
                      Map<String, Map<String, String>> result,
                      Map<String, String> targetFieldToOriginName) {
        if (!(n instanceof LineageTableNode) || StringUtils.isBlank(n.getId())) {
            return;
        }
        String nodeId = n.getId();

        if (targetNodeId.equals(nodeId)) {
            Map<String, String> mapping = new HashMap<>();
            for (String targetField : targetFields) {
                mapping.put(targetField, targetField);
            }
            result.put(nodeId, mapping);
            return;
        }

        Map<String, String> nodeFieldToOrigin = fieldNameMappingByNodeId.get(nodeId);
        if (MapUtils.isEmpty(nodeFieldToOrigin) || MapUtils.isEmpty(targetFieldToOriginName)) {
            return;
        }

        Map<String, String> mapping = new HashMap<>();
        for (Map.Entry<String, String> entry : targetFieldToOriginName.entrySet()) {
            eachTargetFieldToOriginName(entry, nodeFieldToOrigin, mapping);
        }

        if (MapUtils.isNotEmpty(mapping)) {
            keptNodeIds.add(nodeId);
            result.put(nodeId, mapping);
        }
    }

    void eachTargetFieldToOriginName(Map.Entry<String, String> entry, Map<String, String> nodeFieldToOrigin, Map<String, String> mapping) {
        String targetField = entry.getKey();
        String originName = entry.getValue();
        if (StringUtils.isBlank(originName)) {
            return;
        }
        String best = eachNodeFieldToOriginToFindBestName(originName, nodeFieldToOrigin, targetField);
        if (StringUtils.isNotBlank(best)) {
            mapping.put(targetField, best);
        }
    }

    String eachNodeFieldToOriginToFindBestName(String originName, Map<String, String> nodeFieldToOrigin, String targetField) {
        String best = null;
        for (Map.Entry<String, String> e : nodeFieldToOrigin.entrySet()) {
            if (StringUtils.isBlank(e.getKey())
                    || StringUtils.isBlank(e.getValue())
                    || !originName.equals(e.getValue())) {
                continue;
            }
            if (targetField.equals(e.getKey())) {
                return e.getKey();
            }
            if (null == best || e.getKey().compareTo(best) < 0) {
                best = e.getKey();
            }
        }
        return best;
    }

    protected void removeUselessFields(Node<?> node) {
        if (node instanceof LineageTableNode lineageTableNode) {
            LineageMetadataInstance metadata = lineageTableNode.getMetadata();
            if (null == metadata) {
                return;
            }
            metadata.setFields(null);
        }
    }

    protected List<String> getTargetTableUpdateFields(Dag dag, Map<String, DAG> taskDagMap) {
        List<String> targetTableUpdateFields = new ArrayList<>();
        if (null == dag || CollectionUtils.isEmpty(dag.getNodes()) || MapUtils.isEmpty(taskDagMap)) {
            return targetTableUpdateFields;
        }
        LineageTableNode targetLineageTableNode = findFinalTargetLineageTableNode(dag);
        if (null == targetLineageTableNode) {
            return targetTableUpdateFields;
        }
        Map<String, LineageTask> tasks = targetLineageTableNode.getTasks();
        if (MapUtils.isEmpty(tasks)) {
            return targetTableUpdateFields;
        }
        List<LineageTask> taskCandidates = tasks.values()
                .stream()
                .filter(Objects::nonNull)
                .filter(t -> StringUtils.isNotBlank(t.getId()))
                .toList();
        List<LineageTask> targetPosTasks = taskCandidates.stream()
                .filter(t -> isTaskNodeTargetPos(t.getTaskNode()))
                .toList();
        if (CollectionUtils.isNotEmpty(targetPosTasks)) {
            taskCandidates = targetPosTasks;
        }
        eachTaskCandidates(taskCandidates, taskDagMap, targetLineageTableNode, targetTableUpdateFields);
        targetTableUpdateFields = targetTableUpdateFields.stream().filter(StringUtils::isNotBlank).distinct().collect(Collectors.toList());
        return targetTableUpdateFields;
    }

    void eachTaskCandidates(List<LineageTask> taskCandidates, Map<String, DAG> taskDagMap, LineageTableNode targetLineageTableNode, List<String> targetTableUpdateFields) {
        for (LineageTask lineageTask : taskCandidates) {
            String taskId = lineageTask.getId();
            DAG taskDag = taskDagMap.get(taskId);
            if (null == taskDag) {
                continue;
            }
            String effectiveTaskId = null == taskDag.getTaskId() ? taskId : taskDag.getTaskId().toHexString();
            TableNode targetTableNode = findTargetTableNodeInTaskDag(taskDag, targetLineageTableNode.getConnectionId(), targetLineageTableNode.getTable());
            if (null != targetTableNode) {
                List<String> configured = targetTableNode.getUpdateConditionFields();
                if (CollectionUtils.isNotEmpty(configured)) {
                    targetTableUpdateFields.addAll(configured);
                    return;
                }
                List<String> fallback = getPrimaryOrUniqueKeyFieldsForNodeOrSource(effectiveTaskId, targetTableNode);
                if (CollectionUtils.isNotEmpty(fallback)) {
                    targetTableUpdateFields.addAll(fallback);
                    return;
                }
            }
        }
    }

    protected LineageTableNode findFinalTargetLineageTableNode(Dag lineageDag) {
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
        List<LineageTableNode> candidates = CollectionUtils.isEmpty(nonSourceType) ? sinkTableNodes : nonSourceType;
        candidates.sort(Comparator.comparing(Node::getId, Comparator.nullsLast(String::compareTo)));
        return candidates.get(0);
    }

    protected boolean isTaskNodeTargetPos(Node<?> taskNode) {
        if (!(taskNode instanceof com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode)) {
            return false;
        }
        Object pos = ((com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode) taskNode).getTaskNodePos();
        return StringUtils.equalsIgnoreCase(String.valueOf(pos), com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode.TASK_NODE_TARGET_POS);
    }

    protected TableNode findTargetTableNodeInTaskDag(DAG taskDag, String connectionId, String tableName) {
        if (null == taskDag || StringUtils.isBlank(connectionId) || StringUtils.isBlank(tableName)) {
            return null;
        }
        List<Node> targets = taskDag.getTargets();
        if (CollectionUtils.isNotEmpty(targets)) {
            for (Node<?> node : targets) {
                if (!(node instanceof TableNode)) {
                    continue;
                }
                TableNode tableNode = (TableNode) node;
                if (connectionId.equals(tableNode.getConnectionId()) && tableName.equals(tableNode.getTableName())) {
                    return tableNode;
                }
            }
            for (Node<?> node : targets) {
                if (node instanceof TableNode tNode) {
                    return tNode;
                }
            }
        }
        if (CollectionUtils.isNotEmpty(taskDag.getNodes())) {
            for (Node<?> node : taskDag.getNodes()) {
                if (!(node instanceof TableNode)) {
                    continue;
                }
                TableNode tableNode = (TableNode) node;
                if (connectionId.equals(tableNode.getConnectionId()) && tableName.equals(tableNode.getTableName())) {
                    return tableNode;
                }
            }
        }
        return null;
    }

    protected Map<String, DAG> findTaskDagMap(Dag dag) {
        Set<String> taskIds = new HashSet<>();
        dag.getNodes().stream()
                .map(node -> node instanceof LineageTableNode n ? n : null)
                .filter(Objects::nonNull)
                .map(LineageTableNode::getTasks)
                .filter(MapUtils::isNotEmpty)
                .forEach(tasks -> {
                    for (LineageTask lineageTask : tasks.values()) {
                        if (null == lineageTask || StringUtils.isBlank(lineageTask.getId())) {
                            continue;
                        }
                        taskIds.add(lineageTask.getId());
                    }
                });
        return loadTaskDagByTaskId(taskIds);
    }

    protected void markJoinState(Dag dag, Map<String, Map<String, String>> fieldNameMapping, Map<String, DAG> taskDagMap) {
        if (null == dag || CollectionUtils.isEmpty(dag.getNodes())) {
            return;
        }
        Map<String, LineageTableNode> lineageTableNodeByTableKey = new HashMap<>();
        initTableNode(dag, lineageTableNodeByTableKey);
        Map<String, Boolean> hasJoinMap = new HashMap<>();
        Map<String, Boolean> hasMergeMap = new HashMap<>();
        Map<String, Boolean> hasAppendMap = new HashMap<>();
        Map<String, Map<String, TableProperties>> mergeTablePropertiesMap = new HashMap<>();
        Map<String, Map<String, String>> fieldNameMappingByNodeId = MapUtils.isEmpty(fieldNameMapping) ? new HashMap<>() : fieldNameMapping;
        Map<String, Map<String, String>> lineageNodeIdByTaskNodeIdByTaskId = new HashMap<>();
        for (Node<?> n : dag.getNodes()) {
            if (!(n instanceof LineageTableNode tableNode)
                    || MapUtils.isEmpty(tableNode.getTasks())) {
                continue;
            }
            for (LineageTask t : tableNode.getTasks().values()) {
                if (null == t || StringUtils.isBlank(t.getId())
                        || null == t.getTaskNode() || StringUtils.isBlank(t.getTaskNode().getId())) {
                    continue;
                }
                Node<?> taskNode = t.getTaskNode();
                lineageNodeIdByTaskNodeIdByTaskId
                        .computeIfAbsent(t.getId(), k -> new HashMap<>())
                        .putIfAbsent(taskNode.getId(), tableNode.getId());
            }
        }
        taskDagMap.forEach((taskId, taskDag) -> {
            if (null == taskDag || CollectionUtils.isEmpty(taskDag.getNodes())) {
                return;
            }
            Map<String, TableProperties> mergePropertiesMap = mergeTablePropertiesMap.computeIfAbsent(taskId, k -> new HashMap<>());
            Map<String, Node> nodeMap = taskDag.getNodes()
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(Node::getId, n -> n, (n1, n2) -> n2));
            boolean taskHasMerge = false;
            for (Node<?> taskNode : nodeMap.values()) {
                //if (taskNode instanceof JoinProcessorNode) {
                //                    hasJoinMap.put(taskId, true);
                //                    setTableType(taskNode, mergePropertiesMap, taskDag);
                //                } else if (taskNode instanceof MergeTableNode mergeTableNode) {
                //                    hasMergeMap.put(taskId, true);
                //                    List<MergeTableProperties> mergeProperties = mergeTableNode.getMergeProperties();
                //                    collectJoinKeys(mergeProperties, mergePropertiesMap, taskDag, MAIN_TABLE);
                //                    if (containsAppendMergeType(mergeTableNode)) {
                //                        hasAppendMap.put(taskId, true);
                //                    }
                //                } else {
                //                    setTableType(taskNode, mergePropertiesMap, taskDag);
                //}
                if (!(taskNode instanceof MergeTableNode mergeTableNode)) {
                    continue;
                }
                taskHasMerge = true;
                hasMergeMap.put(taskId, true);
                List<MergeTableProperties> mergeProperties = mergeTableNode.getMergeProperties();
                collectJoinKeys(mergeProperties, mergePropertiesMap, taskDag, MAIN_TABLE);
                if (containsAppendMergeType(mergeTableNode)) {
                    hasAppendMap.put(taskId, true);
                }
            }
            if (taskHasMerge) {
                for (Node<?> taskNode : nodeMap.values()) {
                    if (taskNode instanceof JoinProcessorNode) {
                        hasJoinMap.put(taskId, true);
                        setTableType(taskNode, mergePropertiesMap, taskDag);
                    } else if (!(taskNode instanceof MergeTableNode)) {
                        setTableType(taskNode, mergePropertiesMap, taskDag);
                    }
                }
            } else {
                for (Node<?> taskNode : nodeMap.values()) {
                    if (taskNode instanceof JoinProcessorNode) {
                        hasJoinMap.put(taskId, true);
                    }
                }
            }
            if (MapUtils.isEmpty(lineageTableNodeByTableKey) || MapUtils.isEmpty(fieldNameMappingByNodeId)) {
                return;
            }
            TableNode finalTargetTableNode = taskDag.getTargets().stream()
                    .filter(TableNode.class::isInstance)
                    .map(TableNode.class::cast)
                    .findFirst()
                    .orElse(null);
            if (null == finalTargetTableNode) {
                return;
            }
            Map<String, List<String>> targetCandidatesBySourceFieldName = buildTargetCandidatesBySourceFieldName(
                    taskId,
                    finalTargetTableNode,
                    lineageTableNodeByTableKey,
                    fieldNameMappingByNodeId,
                    lineageNodeIdByTaskNodeIdByTaskId
            );
            if (MapUtils.isEmpty(targetCandidatesBySourceFieldName)) {
                return;
            }
            for (Node<?> node : CollectionUtils.emptyIfNull(taskDag.getSources())) {
                if (!(node instanceof TableNode sourceTableNode) || StringUtils.isBlank(sourceTableNode.getId())) {
                    continue;
                }
                String sourceNodeId = sourceTableNode.getId();
                String tableName = sourceTableNode.getTableName();
                JoinProcessorNode firstJoinNode = (JoinProcessorNode) findFirstDownstreamJoinNode(taskDag, sourceNodeId, false);
                if (null != firstJoinNode) {
                    Node<?> firstBeforeJoinNode = findFirstDownstreamJoinNode(taskDag, sourceNodeId, true);
                    firstBeforeJoinNode = firstBeforeJoinNode == null ? node : firstBeforeJoinNode;
                    TableProperties props = mergePropertiesMap.computeIfAbsent(sourceNodeId, k -> {
                        TableProperties p = new TableProperties();
                        p.setRootNodeId(sourceNodeId);
                        p.setPreNodeId(sourceNodeId);
                        return p;
                    });
                    List<String> joinKeys = extractJoinKeyFieldNamesForSource(taskDag, sourceNodeId, firstJoinNode);
                    if (CollectionUtils.isNotEmpty(joinKeys)) {
                        List<FieldNameMapping> mappings = new ArrayList<>();
                        Map<String, Map<String, Field>> fieldsByNodeIdCache = new HashMap<>();
                        for (String originName : joinKeys) {
                            String realOriginName = getOriginFieldName(taskId, firstBeforeJoinNode.getId(), tableName, originName, fieldsByNodeIdCache);
                            String targetFieldName = pickBestTargetFieldName(targetCandidatesBySourceFieldName.get(realOriginName), realOriginName);
                            if (StringUtils.isBlank(targetFieldName)) {
                                targetFieldName = originName;
                            }
                            addFieldNameMapping(mappings, realOriginName, targetFieldName);
                        }
                        props.setJoinKeys(mappings);
                    }
                }
            }
        });
        List<Node> nodes = dag.getNodes();
        for (Node<?> node : nodes) {
            mergeInfoSetter(node, hasJoinMap, hasMergeMap, hasAppendMap, mergeTablePropertiesMap);
        }
    }

    void setTableType(Node<?> node, Map<String, TableProperties> mergeTablePropertiesMap, DAG dag) {
        //1. 找出当前node的下游节点中是否出现合并节点，如果出现,先设置nodeType=NODE_ATTR_TYPE_MERGE，再根据当前节点对应的TableProperties设置tableType
        //1-1. 如果当前node时合并节点对应的主表链路，则对应的tableType设置成MAIN_TABLE
        //1-2. 如果当前node时合并节点对应的子表链路，则对应的tableType设置成SUB_TABLE
        if (null == node || null == dag || null == mergeTablePropertiesMap) {
            return;
        }
        String nodeId = node.getId();
        if (StringUtils.isBlank(nodeId)) {
            return;
        }
        Set<String> visited = new HashSet<>();
        Queue<String> queue = new java.util.ArrayDeque<>();
        visited.add(nodeId);
        queue.add(nodeId);

        boolean hasDownstreamMergeNode = false;
        String resolvedTableType = null;

        while (!queue.isEmpty()) {
            String currentId = queue.poll();
            if (StringUtils.isBlank(currentId)) {
                continue;
            }
            Node<?> current = dag.getNode(currentId);
            if (current instanceof MergeTableNode mergeTableNode) {
                hasDownstreamMergeNode = true;
                Set<String> mainTableNodeIds = new HashSet<>();
                Set<String> subTableNodeIds = new HashSet<>();
                List<MergeTableProperties> mergeProperties = mergeTableNode.getMergeProperties();
                if (CollectionUtils.isNotEmpty(mergeProperties)) {
                    for (MergeTableProperties p : mergeProperties) {
                        if (null == p) {
                            continue;
                        }
                        if (StringUtils.isNotBlank(p.getId())) {
                            mainTableNodeIds.add(p.getId());
                        }
                        collectMergePropertiesIds(p.getChildren(), subTableNodeIds);
                    }
                }
                if (!mainTableNodeIds.isEmpty() || !subTableNodeIds.isEmpty()) {
                    boolean inMain = visited.stream().anyMatch(mainTableNodeIds::contains);
                    boolean inSub = !inMain && visited.stream().anyMatch(subTableNodeIds::contains);
                    if (inMain) {
                        resolvedTableType = MAIN_TABLE;
                    } else if (inSub) {
                        resolvedTableType = SUB_TABLE;
                    }
                }
                break;
            }
            List<Node> successors = dag.getTarget(currentId);
            if (CollectionUtils.isEmpty(successors)) {
                continue;
            }
            for (Node<?> succ : successors) {
                if (null == succ || StringUtils.isBlank(succ.getId())) {
                    continue;
                }
                String succId = succ.getId();
                if (visited.add(succId)) {
                    queue.add(succId);
                }
            }
        }

        if (!hasDownstreamMergeNode) {
            return;
        }
        TableProperties properties = mergeTablePropertiesMap.computeIfAbsent(nodeId, k -> newTableProperties(nodeId, nodeId));
        properties.setNodeType(NODE_ATTR_TYPE_MERGE);
        if (StringUtils.isBlank(properties.getTableType()) && StringUtils.isNotBlank(resolvedTableType)) {
            properties.setTableType(resolvedTableType);
        }
    }

    void collectMergePropertiesIds(List<MergeTableProperties> propertiesList, Set<String> outIds) {
        if (CollectionUtils.isEmpty(propertiesList) || null == outIds) {
            return;
        }
        for (MergeTableProperties p : propertiesList) {
            if (null == p) {
                continue;
            }
            if (StringUtils.isNotBlank(p.getId())) {
                outIds.add(p.getId());
            }
            if (CollectionUtils.isNotEmpty(p.getChildren())) {
                collectMergePropertiesIds(p.getChildren(), outIds);
            }
        }
    }

    void collectChildrenJoinKeys(List<MergeTableProperties> children, Map<String, TableProperties> mergePropertiesMap, DAG taskDag) {
        if (null == children || children.isEmpty()) {
            return;
        }
        collectJoinKeys(children, mergePropertiesMap, taskDag, SUB_TABLE);
    }

    void collectJoinKeys(List<MergeTableProperties> infos, Map<String, TableProperties> mergePropertiesMap, DAG taskDag, String tableType) {
        infos.forEach(child -> {
            String id = child.getId();
            Map<String, TableProperties> propertiesMap = loadRootInfo(id, taskDag, child);
            propertiesMap.forEach((k, v) -> v.setTableType(tableType));
            mergePropertiesMap.putAll(propertiesMap);
            List<MergeTableProperties> sub = child.getChildren();
            collectChildrenJoinKeys(sub, mergePropertiesMap, taskDag);
        });
    }

    void initTableNode(Dag dag, Map<String, LineageTableNode> lineageTableNodeByTableKey) {
        for (Node<?> n : dag.getNodes()) {
            if (!(n instanceof LineageTableNode tableNode)) {
                continue;
            }
            String tableKey = toTableKey(tableNode.getConnectionId(), tableNode.getTable());
            if (StringUtils.isNotBlank(tableKey)) {
                lineageTableNodeByTableKey.putIfAbsent(tableKey, tableNode);
            }
        }
    }

    void mergeInfoSetter(Node<?> n, Map<String, Boolean> hasJoinMap, Map<String, Boolean> hasMergeMap, Map<String, Boolean> hasAppendMap, Map<String, Map<String, TableProperties>> mergeTablePropertiesMap) {
        if (!(n instanceof LineageTableNode node) || null == node.getMetadata()) {
            return;
        }
        Map<String, LineageTask> tasks = node.getTasks();
        if (MapUtils.isEmpty(tasks)) {
            return;
        }
        for (LineageTask lineageTask : tasks.values()) {
            if (null == lineageTask || StringUtils.isBlank(lineageTask.getId())) {
                continue;
            }
            String taskId = lineageTask.getId();
            Node<?> taskNode = lineageTask.getTaskNode();
            String nodeId = taskNode.getId();
            boolean hasJoin = Optional.ofNullable(hasJoinMap.get(taskId)).orElse(false);
            boolean hasMerge = Optional.ofNullable(hasMergeMap.get(taskId)).orElse(false);
            boolean hasAppend = Optional.ofNullable(hasAppendMap.get(taskId)).orElse(false);
            final String nodeType = nodeType(hasJoin, hasMerge, hasAppend);
            Map<String, TableProperties> propertiesMap = mergeTablePropertiesMap.get(taskId);
            Map<String, Object> attrs = node.getAttrs();
            if (null == attrs) {
                attrs = new HashMap<>();
                node.setAttrs(attrs);
            }
            Optional.ofNullable(propertiesMap.get(nodeId))
                    .ifPresent(properties -> {
                        if (StringUtils.isBlank(properties.getNodeType())) {
                            properties.setNodeType(nodeType);
                        }
                        node.getAttrs().put(taskId, properties);
                    });
        }
    }

    String nodeType(boolean hasJoin, boolean hasMerge, boolean hasAppend) {
        if (hasJoin) {
            return NODE_ATTR_TYPE_JOIN;
        } else if (hasMerge) {
            return NODE_ATTR_TYPE_MERGE;
        } else if (hasAppend) {
            return NODE_ATTR_TYPE_APPEND;
        } else {
            return NODE_ATTR_TYPE_OTHER;
        }
    }

    private Map<String, List<String>> buildTargetCandidatesBySourceFieldName(
            String taskId,
            TableNode finalTargetTableNode,
            Map<String, LineageTableNode> lineageTableNodeByTableKey,
            Map<String, Map<String, String>> fieldNameMappingByNodeId,
            Map<String, Map<String, String>> lineageNodeIdByTaskNodeIdByTaskId
    ) {
        if (StringUtils.isBlank(taskId) || null == finalTargetTableNode) {
            return new HashMap<>();
        }
        String targetTableKey = toTableKey(finalTargetTableNode.getConnectionId(), finalTargetTableNode.getTableName());
        String targetLineageNodeId = Optional.ofNullable(lineageTableNodeByTableKey.get(targetTableKey)).map(Node::getId).orElse(null);
        if (StringUtils.isBlank(targetLineageNodeId)) {
            targetLineageNodeId = Optional.ofNullable(lineageNodeIdByTaskNodeIdByTaskId.get(taskId))
                    .map(m -> m.get(finalTargetTableNode.getId()))
                    .orElse(null);
        }
        Map<String, String> targetFieldNameMapping = StringUtils.isBlank(targetLineageNodeId)
                ? new HashMap<>()
                : fieldNameMappingByNodeId.getOrDefault(targetLineageNodeId, new HashMap<>());
        Map<String, List<String>> targetCandidatesBySourceFieldName = new HashMap<>();
        if (MapUtils.isNotEmpty(targetFieldNameMapping)) {
            targetFieldNameMapping.forEach((targetFieldName, sourceFieldName) -> {
                if (StringUtils.isBlank(targetFieldName) || StringUtils.isBlank(sourceFieldName)) {
                    return;
                }
                targetCandidatesBySourceFieldName.computeIfAbsent(sourceFieldName, k -> new ArrayList<>()).add(targetFieldName);
            });
        }
        return targetCandidatesBySourceFieldName;
    }

    private Node<?> findFirstDownstreamJoinNode(DAG taskDag, String startNodeId, boolean before) {
        if (null == taskDag || StringUtils.isBlank(startNodeId)) {
            return null;
        }
        Set<String> visited = new HashSet<>();
        Queue<String> queue = new java.util.ArrayDeque<>();
        visited.add(startNodeId);
        queue.add(startNodeId);
        Node<?> beforeNode = null;
        while (!queue.isEmpty()) {
            String current = queue.poll();
            if (StringUtils.isBlank(current)) {
                continue;
            }
            List<Node> successors = taskDag.getTarget(current);
            if (CollectionUtils.isEmpty(successors)) {
                continue;
            }
            for (Node<?> succ : successors) {
                if (null == succ || StringUtils.isBlank(succ.getId()) || !visited.add(succ.getId())) {
                    continue;
                }
                if (succ instanceof JoinProcessorNode joinProcessorNode) {
                    return before ? beforeNode : joinProcessorNode;
                }
                beforeNode = succ;
                queue.add(succ.getId());
            }
        }
        return null;
    }

    private boolean isAncestorInTaskDag(DAG taskDag, String ancestorId, String nodeId) {
        if (null == taskDag || StringUtils.isBlank(ancestorId) || StringUtils.isBlank(nodeId)) {
            return false;
        }
        if (ancestorId.equals(nodeId)) {
            return true;
        }
        Set<String> visited = new HashSet<>();
        Queue<String> queue = new java.util.ArrayDeque<>();
        visited.add(nodeId);
        queue.add(nodeId);
        while (!queue.isEmpty()) {
            String current = queue.poll();
            List<Node> pres = taskDag.predecessors(current);
            if (CollectionUtils.isEmpty(pres)) {
                continue;
            }
            for (Node<?> pre : pres) {
                if (null == pre || StringUtils.isBlank(pre.getId())) {
                    continue;
                }
                if (ancestorId.equals(pre.getId())) {
                    return true;
                }
                if (visited.add(pre.getId())) {
                    queue.add(pre.getId());
                }
            }
        }
        return false;
    }

    private List<String> extractJoinKeyFieldNamesForSource(DAG taskDag, String sourceNodeId, JoinProcessorNode joinNode) {
        if (null == taskDag || StringUtils.isBlank(sourceNodeId) || null == joinNode || CollectionUtils.isEmpty(joinNode.getJoinExpressions())) {
            return new ArrayList<>();
        }
        String leftNodeId = joinNode.getLeftNodeId();
        String rightNodeId = joinNode.getRightNodeId();
        boolean inLeft = StringUtils.isNotBlank(leftNodeId) && isAncestorInTaskDag(taskDag, sourceNodeId, leftNodeId);
        boolean inRight = StringUtils.isNotBlank(rightNodeId) && isAncestorInTaskDag(taskDag, sourceNodeId, rightNodeId);
        if (!inLeft && !inRight) {
            return new ArrayList<>();
        }
        List<String> keys = new ArrayList<>();
        for (JoinProcessorNode.JoinExpression expr : joinNode.getJoinExpressions()) {
            if (null == expr) {
                continue;
            }
            if (inLeft && StringUtils.isNotBlank(expr.getLeft())) {
                keys.add(expr.getLeft());
            }
            if (inRight && StringUtils.isNotBlank(expr.getRight())) {
                keys.add(expr.getRight());
            }
        }
        return keys.stream().filter(StringUtils::isNotBlank).distinct().toList();
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

    protected String pickBestTargetFieldName(List<String> targetCandidates, String sourceFieldName) {
        if (CollectionUtils.isEmpty(targetCandidates) || StringUtils.isBlank(sourceFieldName)) {
            return null;
        }
        if (targetCandidates.size() == 1) {
            return targetCandidates.get(0);
        }
        for (String target : targetCandidates) {
            if (sourceFieldName.equals(target)) {
                return target;
            }
        }
        List<String> sorted = targetCandidates.stream().filter(StringUtils::isNotBlank).distinct().sorted(String::compareTo).toList();
        return sorted.isEmpty() ? null : sorted.get(0);
    }

    protected Map<String, TableProperties> loadRootInfo(String preNodeId, DAG taskDag, MergeTableProperties mergeTableProperties) {
        Map<String, TableProperties> result = new HashMap<>();
        if (StringUtils.isBlank(preNodeId) || null == taskDag || null == taskDag.getTaskId() || null == mergeTableProperties) {
            return result;
        }
        Set<String> rootNodeIds = findRootNodeIds(taskDag, preNodeId);
        String taskId = taskDag.getTaskId().toHexString();
        Map<String, Map<String, Field>> fieldsByNodeIdCache = new HashMap<>();
        String preTableName;
        if (rootNodeIds.isEmpty() || (rootNodeIds.size() == 1 && !rootNodeIds.contains(preNodeId))) {
            String rootNodeId = rootNodeIds.stream().findFirst().orElse(preNodeId);
            preTableName = resolveNodeTableName(taskDag, rootNodeId);
            TableProperties isSelf = new TableProperties();
            isSelf.setRootNodeId(rootNodeId);
            isSelf.setPreNodeId(preNodeId);
            isSelf.setPath(resolveMergePropertiesPath(mergeTableProperties));
            List<Map<String, String>> joinKeys = Optional.ofNullable(mergeTableProperties.getJoinKeys())
                    .orElse(new ArrayList<>())
                    .stream()
                    .filter(Objects::nonNull)
                    .filter(MapUtils::isNotEmpty)
                    .toList();
            isSelfSetJoinKey(isSelf, taskId, preNodeId, preTableName, fieldsByNodeIdCache, joinKeys);
            isSelfSetTablePk(isSelf, taskId, preNodeId, preTableName, fieldsByNodeIdCache, mergeTableProperties);
            isSelf.setTableType(SUB_TABLE);
            result.put(rootNodeId, isSelf);
            return result;
        } else {
            preTableName = resolveNodeTableName(taskDag, preNodeId);
        }
        eachArrayKeys(taskDag, preNodeId, preTableName, fieldsByNodeIdCache, mergeTableProperties, result);
        resultSetJoinKey(taskDag, preNodeId, preTableName, fieldsByNodeIdCache, mergeTableProperties, result);
        return result;
    }

    void isSelfSetJoinKey(TableProperties isSelf, String taskId, String preNodeId, String preTableName, Map<String, Map<String, Field>> fieldsByNodeIdCache, List<Map<String, String>> joinKeys) {
        if (CollectionUtils.isEmpty(joinKeys)) {
            return;
        }
        List<FieldNameMapping> joinKeyMappings = new ArrayList<>();
        for (Map<String, String> joinKey : joinKeys) {
            String sourceName = joinKey.get("source");
            String targetName = joinKey.get("target");
            if (StringUtils.isBlank(targetName) || StringUtils.isBlank(sourceName)) {
                continue;
            }
            String originName = getOriginFieldName(taskId, preNodeId, preTableName, sourceName, fieldsByNodeIdCache);
            addFieldNameMapping(joinKeyMappings, originName, targetName);
        }
        isSelf.setJoinKeys(joinKeyMappings);
    }

    void isSelfSetTablePk(TableProperties isSelf, String taskId, String preNodeId, String preTableName, Map<String, Map<String, Field>> fieldsByNodeIdCache, MergeTableProperties mergeTableProperties) {
        List<String> arrayKeys = mergeTableProperties.getArrayKeys();
        if (CollectionUtils.isNotEmpty(arrayKeys)) {
            List<FieldNameMapping> tablePkMappings = new ArrayList<>();
            for (String targetName : arrayKeys) {
                if (StringUtils.isBlank(targetName)) {
                    continue;
                }
                String originName = getOriginFieldName(taskId, preNodeId, preTableName, targetName, fieldsByNodeIdCache);
                addFieldNameMapping(tablePkMappings, originName, targetName);
            }
            isSelf.setTablePk(tablePkMappings);
        }
    }

    void eachArrayKeys(DAG taskDag, String preNodeId, String preTableName, Map<String, Map<String, Field>> fieldsByNodeIdCache, MergeTableProperties mergeTableProperties, Map<String, TableProperties> result) {
        List<String> arrayKeys = mergeTableProperties.getArrayKeys();
        if (CollectionUtils.isEmpty(arrayKeys)) {
            return;
        }
        String taskId = taskDag.getTaskId().toHexString();
        for (String arrayKey : arrayKeys) {
            if (StringUtils.isBlank(arrayKey)) {
                continue;
            }
            Optional<TracedField> traced = traceToRoot(taskDag, taskId, preNodeId, preTableName, arrayKey, fieldsByNodeIdCache);
            traced.ifPresent(tf -> {
                TableProperties tableProperties = result.computeIfAbsent(tf.rootNodeId, k -> newTableProperties(tf.rootNodeId, preNodeId));
                if (null == tableProperties.getTablePk()) {
                    tableProperties.setTablePk(new ArrayList<>());
                }
                if (StringUtils.isBlank(tableProperties.getPath())) {
                    tableProperties.setPath(resolveMergePropertiesPath(mergeTableProperties));
                }
                addFieldNameMapping(tableProperties.getTablePk(), tf.rootFieldName, arrayKey);
            });
        }
    }

    void resultSetJoinKey(DAG taskDag, String preNodeId, String preTableName, Map<String, Map<String, Field>> fieldsByNodeIdCache, MergeTableProperties mergeTableProperties, Map<String, TableProperties> result) {
        List<Map<String, String>> joinKeys = Optional.ofNullable(mergeTableProperties.getJoinKeys())
                .orElse(new ArrayList<>())
                .stream()
                .filter(Objects::nonNull)
                .filter(MapUtils::isNotEmpty)
                .toList();
        if (CollectionUtils.isEmpty(joinKeys)) {
            return;
        }
        String taskId = taskDag.getTaskId().toHexString();
        for (Map<String, String> joinKey : joinKeys) {
            String sourceName = joinKey.get("source");
            String targetName = joinKey.get("target");
            if (StringUtils.isBlank(sourceName) || StringUtils.isBlank(targetName)) {
                continue;
            }
            Optional<TracedField> traced = traceToRoot(taskDag, taskId, preNodeId, preTableName, sourceName, fieldsByNodeIdCache);
            traced.ifPresent(tf -> {
                TableProperties tableProperties = result.computeIfAbsent(tf.rootNodeId, k -> newTableProperties(tf.rootNodeId, preNodeId));
                if (null == tableProperties.getJoinKeys()) {
                    tableProperties.setJoinKeys(new ArrayList<>());
                }
                if (StringUtils.isBlank(tableProperties.getPath())) {
                    tableProperties.setPath(resolveMergePropertiesPath(mergeTableProperties));
                }
                addFieldNameMapping(tableProperties.getJoinKeys(), tf.rootFieldName, targetName);
            });
        }
    }

    private String resolveMergePropertiesPath(MergeTableProperties mergeTableProperties) {
        if (null == mergeTableProperties) {
            return null;
        }
        return StringUtils.defaultIfBlank(mergeTableProperties.getArrayPath(), mergeTableProperties.getTargetPath());
    }

    protected Map<String, Map<String, String>> groupFieldOriginalNameMappingByNodeId(List<Node> nodes) {
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

    protected Map<String, DAG> loadTaskDagByTaskId(Collection<String> taskId) {
        List<ObjectId> oIds = taskId.stream().map(MongoUtils::toObjectId).filter(Objects::nonNull).toList();
        if (CollectionUtils.isEmpty(oIds)) {
            return new HashMap<>();
        }
        Query query = new Query(Criteria.where("_id").in(oIds));
        query.fields().include("dag", "_id");
        List<TaskDto> taskDto = taskService.findAll(query);
        return taskDto.stream().filter(Objects::nonNull)
                .collect(Collectors.toMap(e -> e.getId().toHexString(), TaskDto::getDag, (t1, t2) -> t2));
    }

    protected boolean containsAppendMergeType(MergeTableNode mergeTableNode) {
        if (null == mergeTableNode || CollectionUtils.isEmpty(mergeTableNode.getMergeProperties())) {
            return false;
        }
        return flattenMergeProperties(mergeTableNode.getMergeProperties()).stream()
                .map(MergeTableProperties::getMergeType)
                .filter(Objects::nonNull)
                .anyMatch(t -> t == MergeTableProperties.MergeType.appendWrite);
    }

    protected List<MergeTableProperties> flattenMergeProperties(List<MergeTableProperties> mergeProperties) {
        List<MergeTableProperties> list = new ArrayList<>();
        if (CollectionUtils.isEmpty(mergeProperties)) {
            return list;
        }
        for (MergeTableProperties properties : mergeProperties) {
            if (null == properties) {
                continue;
            }
            list.add(properties);
            if (CollectionUtils.isNotEmpty(properties.getChildren())) {
                list.addAll(flattenMergeProperties(properties.getChildren()));
            }
        }
        return list;
    }

    protected TableProperties newTableProperties(String rootNodeId, String preNodeId) {
        TableProperties tableProperties = new TableProperties();
        tableProperties.setRootNodeId(rootNodeId);
        tableProperties.setPreNodeId(preNodeId);
        return tableProperties;
    }

    protected void addFieldNameMapping(List<FieldNameMapping> list, String originName, String targetName) {
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

    protected String getOriginFieldName(String taskId, String nodeId, String tableName, String targetName, Map<String, Map<String, Field>> fieldsByNodeIdCache) {
        Field field = getField(taskId, nodeId, tableName, targetName, fieldsByNodeIdCache);
        if (null != field && StringUtils.isNotBlank(field.getOriginalFieldName())) {
            return field.getOriginalFieldName();
        }
        return targetName;
    }

    protected String toTableKey(String connectionId, String tableName) {
        if (StringUtils.isBlank(connectionId) || StringUtils.isBlank(tableName)) {
            return null;
        }
        return connectionId + "_" + tableName;
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

    protected List<String> getPrimaryOrUniqueKeyFieldsForNodeOrSource(String taskId, TableNode tableNode) {
        if (null == tableNode) {
            return new ArrayList<>();
        }
        String nodeId = tableNode.getId();
        return getPrimaryOrUniqueKeyFieldsForNodeOrSource(taskId, nodeId, tableNode.getConnectionId(), tableNode.getTableName());
    }

    protected List<String> getPrimaryOrUniqueKeyFieldsForNodeOrSource(String taskId, String nodeId, String connectionId, String tableName) {
        List<String> fields = getPrimaryOrUniqueKeyFields(taskId, nodeId);
        if (CollectionUtils.isNotEmpty(fields)) {
            return fields;
        }
        return getPrimaryOrUniqueKeyFieldsFromSource(connectionId, tableName);
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

    protected List<String> extractPrimaryOrUniqueKeyFields(List<Field> fields, List<TableIndex> indices) {
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

    protected Set<String> findRootNodeIds(DAG taskDag, String preNodeId) {
        Set<String> roots = new HashSet<>();
        if (null == taskDag || StringUtils.isBlank(preNodeId) || !taskDag.hasNode(preNodeId)) {
            return roots;
        }
        Set<String> visited = new HashSet<>();
        Queue<String> queue = new java.util.ArrayDeque<>();
        queue.add(preNodeId);
        visited.add(preNodeId);
        while (!queue.isEmpty()) {
            String currentId = queue.poll();
            List<Node> predecessors = taskDag.predecessors(currentId);
            if (CollectionUtils.isEmpty(predecessors)) {
                roots.add(currentId);
                continue;
            }
            for (Node<?> predecessor : predecessors) {
                if (null == predecessor || StringUtils.isBlank(predecessor.getId())) {
                    continue;
                }
                String pid = predecessor.getId();
                if (visited.add(pid)) {
                    queue.add(pid);
                }
            }
        }
        return roots;
    }

    protected Optional<TracedField> traceToRoot(
            DAG taskDag,
            String taskId,
            String startNodeId,
            String startTableName,
            String fieldName,
            Map<String, Map<String, Field>> fieldsByNodeIdCache
    ) {
        if (null == taskDag || StringUtils.isBlank(taskId) || StringUtils.isBlank(startNodeId) || StringUtils.isBlank(fieldName)) {
            return Optional.empty();
        }
        String currentNodeId = startNodeId;
        String currentTableName = startTableName;
        String currentFieldName = fieldName;
        Set<String> visitedNodeIds = new HashSet<>();
        for (int step = 0; step < 64; step++) {
            if (!visitedNodeIds.add(currentNodeId)) {
                return Optional.empty();
            }
            Field field = getField(taskId, currentNodeId, currentTableName, currentFieldName, fieldsByNodeIdCache);
            if (null == field) {
                return Optional.empty();
            }
            List<Node> predecessors = taskDag.predecessors(currentNodeId);
            if (CollectionUtils.isEmpty(predecessors)) {
                return Optional.of(new TracedField(currentNodeId, currentFieldName));
            }
            predecessors = predecessors.stream()
                    .filter(Objects::nonNull)
                    .filter(predecessor -> StringUtils.isBlank(predecessor.getId()))
                    .toList();
            String previousFieldName = field.getPreviousFieldName();
            NodeFieldState next = null;
            if (StringUtils.isNotBlank(previousFieldName)) {
                List<NodeFieldState> candidates = new ArrayList<>();
                for (Node<?> predecessor : predecessors) {
                    if (null == predecessor || StringUtils.isBlank(predecessor.getId())) {
                        continue;
                    }
                    String preTableName = resolvePredecessorTableName(taskDag, predecessor.getId(), currentTableName);
                    Field preField = getField(taskId, predecessor.getId(), preTableName, previousFieldName, fieldsByNodeIdCache);
                    if (null != preField) {
                        candidates.add(new NodeFieldState(predecessor.getId(), preTableName, previousFieldName));
                    }
                }
                if (!candidates.isEmpty()) {
                    if (candidates.size() == 1) {
                        next = candidates.get(0);
                    } else {
                        NodeFieldState matched = null;
                        for (NodeFieldState candidate : candidates) {
                            Field preField = getField(taskId, candidate.nodeId, candidate.tableName, previousFieldName, fieldsByNodeIdCache);
                            if (isSameFieldLineage(field, preField)) {
                                if (matched != null) {
                                    return Optional.empty();
                                }
                                matched = candidate;
                            }
                        }
                        next = matched;
                    }
                }
            }
            if (next == null) {
                next = matchedNodeFieldState(taskId, currentTableName, predecessors, taskDag, fieldsByNodeIdCache, field);
                if (next == null) {
                    return Optional.empty();
                }
            }
            currentNodeId = next.nodeId;
            currentTableName = next.tableName;
            currentFieldName = next.fieldName;
        }
        return Optional.empty();
    }

    NodeFieldState matchedNodeFieldState(String taskId, String currentTableName, List<Node> predecessors, DAG taskDag, Map<String, Map<String, Field>> fieldsByNodeIdCache, Field field) {
        NodeFieldState matched = null;
        for (Node<?> predecessor : predecessors) {
            String preTableName = resolvePredecessorTableName(taskDag, predecessor.getId(), currentTableName);
            Map<String, Field> preFieldMap = getFieldMapForNode(taskId, predecessor.getId(), preTableName, fieldsByNodeIdCache);
            if (MapUtils.isEmpty(preFieldMap)) {
                continue;
            }
            for (Field preField : preFieldMap.values()) {
                if (null == preField || StringUtils.isBlank(preField.getFieldName())) {
                    continue;
                }
                if (isSameFieldLineage(field, preField)) {
                    if (matched != null) {
                        return null;
                    }
                    matched = new NodeFieldState(predecessor.getId(), preTableName, preField.getFieldName());
                }
            }
        }
        return matched;
    }

    protected boolean isSameFieldLineage(Field currentField, Field preField) {
        if (null == currentField || null == preField) {
            return false;
        }
        Set<String> currentIds = new HashSet<>();
        if (StringUtils.isNotBlank(currentField.getId())) {
            currentIds.add(currentField.getId());
        }
        if (CollectionUtils.isNotEmpty(currentField.getOldIdList())) {
            currentIds.addAll(currentField.getOldIdList());
        }
        if (currentIds.isEmpty()) {
            return false;
        }
        if (StringUtils.isNotBlank(preField.getId()) && currentIds.contains(preField.getId())) {
            return true;
        }
        if (CollectionUtils.isNotEmpty(preField.getOldIdList())) {
            for (String oldId : preField.getOldIdList()) {
                if (StringUtils.isNotBlank(oldId) && currentIds.contains(oldId)) {
                    return true;
                }
            }
        }
        return false;
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

    protected Field getField(String taskId, String nodeId, String tableName, String fieldName, Map<String, Map<String, Field>> fieldsByNodeIdCache) {
        if (StringUtils.isBlank(taskId) || StringUtils.isBlank(nodeId) || StringUtils.isBlank(fieldName)) {
            return null;
        }
        Map<String, Field> fieldMap = getFieldMapForNode(taskId, nodeId, tableName, fieldsByNodeIdCache);
        return fieldMap.get(fieldName);
    }

    protected String resolveNodeTableName(DAG taskDag, String nodeId) {
        if (null == taskDag || StringUtils.isBlank(nodeId)) {
            return null;
        }
        Node<?> node = taskDag.getNode(nodeId);
        if (node instanceof TableNode tableNode) {
            return tableNode.getTableName();
        }
        return null;
    }

    protected String resolvePredecessorTableName(DAG taskDag, String preNodeId, String currentTableName) {
        String resolved = resolveNodeTableName(taskDag, preNodeId);
        if (StringUtils.isNotBlank(resolved)) {
            return resolved;
        }
        return currentTableName;
    }

    @Data
    public static class TableProperties {
        String rootNodeId;
        String preNodeId;
        String nodeType;
        List<FieldNameMapping> joinKeys;
        List<FieldNameMapping> tablePk;
        List<String> updateConditionField;
        String path;
        String tableType;
    }

    @Data
    public static class FieldNameMapping {
        String originName;
        String targetName;
    }

    @Data
    protected static final class TracedField {
        private String rootNodeId;
        private String rootFieldName;

        public TracedField(String rootNodeId, String rootFieldName) {
            this.rootNodeId = rootNodeId;
            this.rootFieldName = rootFieldName;
        }
    }

    @Data
    protected static final class NodeFieldState {
        private final String nodeId;
        private final String tableName;
        private final String fieldName;

        private NodeFieldState(String nodeId, String tableName, String fieldName) {
            this.nodeId = nodeId;
            this.tableName = tableName;
            this.fieldName = fieldName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NodeFieldState that = (NodeFieldState) o;
            return Objects.equals(nodeId, that.nodeId)
                    && Objects.equals(tableName, that.tableName)
                    && Objects.equals(fieldName, that.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, tableName, fieldName);
        }
    }
}
