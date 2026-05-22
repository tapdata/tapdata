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
 * @description node.attr.type: <$taskId, ['JOIN' , 'MERGE' , 'APPEND' , 'OTHER']>
 * node.attr.joinKeys: <$taskId, ['xxx'...]>
 * node.attr.tablePk: <$taskId, ['yyy'...]>
 */
@Service
@Slf4j
public class BloodlineFinder {
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
        Map<String, Map<String, String>> fieldNameMapping = groupFieldNameMappingByNodeId(dag.getNodes());
        Map<String, DAG> taskDagMap = markJoinState(dag, fieldNameMapping);
        Map<String, List<FieldNameMapping>> updateConditionFieldList = getUpdateConditionFieldList(dag, taskDagMap, fieldNameMapping);
        taskLineage.setUpdateConditionFieldList(updateConditionFieldList);
        taskLineage.setFieldNameMapping(fieldNameMapping);
        removeUselessFields(dag);
        return taskLineage;
    }

    public TargetWithLineageDto findTaskLineageSimply(TaskLineageParam param) {
        TargetWithLineageDto taskLineage = new TargetWithLineageDto(findLineage(param));
        Dag dag = taskLineage.getDag();
        Map<String, Map<String, String>> fieldNameMapping = groupFieldNameMappingByNodeId(dag.getNodes());
        Map<String, DAG> taskDagMap = markJoinState(dag, fieldNameMapping);
        List<String> targetTableUpdateFields = getTargetTableUpdateFields(dag, taskDagMap);
        taskLineage.setTargetTableUpdateFields(targetTableUpdateFields);
        removeUselessFields(dag);
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

    protected void removeUselessFields(Dag dag) {
        if (null == dag || null == dag.getNodes()) {
            return;
        }
        dag.getNodes().forEach(this::removeUselessFields);
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
                .collect(Collectors.toList());
        List<LineageTask> targetPosTasks = taskCandidates.stream()
                .filter(t -> isTaskNodeTargetPos(t.getTaskNode()))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(targetPosTasks)) {
            taskCandidates = targetPosTasks;
        }
        for (LineageTask lineageTask : taskCandidates) {
            String taskId = lineageTask.getId();
            DAG taskDag = taskDagMap.get(taskId);
            if (null == taskDag) {
                continue;
            }
            String effectiveTaskId = null == taskDag.getTaskId() ? taskId : taskDag.getTaskId().toHexString();
            TableNode targetTableNode = findTargetTableNodeInTaskDag(taskDag, targetLineageTableNode.getConnectionId(), targetLineageTableNode.getTable());
            if (null == targetTableNode) {
                continue;
            }
            List<String> configured = targetTableNode.getUpdateConditionFields();
            if (CollectionUtils.isNotEmpty(configured)) {
                targetTableUpdateFields.addAll(configured);
                break;
            }
            List<String> fallback = getPrimaryOrUniqueKeyFieldsForNodeOrSource(effectiveTaskId, targetTableNode);
            if (CollectionUtils.isNotEmpty(fallback)) {
                targetTableUpdateFields.addAll(fallback);
                break;
            }
        }
        targetTableUpdateFields = targetTableUpdateFields.stream().filter(StringUtils::isNotBlank).distinct().collect(Collectors.toList());
        return targetTableUpdateFields;
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
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(sinkTableNodes)) {
            sinkTableNodes = new ArrayList<>(tableNodeById.values());
        }
        if (sinkTableNodes.size() == 1) {
            return sinkTableNodes.get(0);
        }
        List<LineageTableNode> nonSourceType = sinkTableNodes.stream()
                .filter(n -> null == n.getMetadata() || !"SOURCE".equalsIgnoreCase(n.getMetadata().getSourceType()))
                .collect(Collectors.toList());
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
                if (node instanceof TableNode) {
                    return (TableNode) node;
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

    protected Map<String, DAG> markJoinState(Dag dag, Map<String, Map<String, String>> fieldNameMapping) {
        if (null == dag || CollectionUtils.isEmpty(dag.getNodes())) {
            return new HashMap<>();
        }
        Map<String, LineageTableNode> lineageTableNodeByTableKey = new HashMap<>();
        for (Node<?> n : dag.getNodes()) {
            if (n instanceof LineageTableNode tableNode) {
                String tableKey = toTableKey(tableNode.getConnectionId(), tableNode.getTable());
                if (StringUtils.isNotBlank(tableKey)) {
                    lineageTableNodeByTableKey.putIfAbsent(tableKey, tableNode);
                }
            }
        }
        Set<String> taskIds = new HashSet<>();
        for (Node<?> node : dag.getNodes()) {
            if (!(node instanceof LineageTableNode)) {
                continue;
            }
            Map<String, LineageTask> tasks = ((LineageTableNode) node).getTasks();
            if (MapUtils.isEmpty(tasks)) {
                continue;
            }
            for (LineageTask lineageTask : tasks.values()) {
                if (null == lineageTask || StringUtils.isBlank(lineageTask.getId())) {
                    continue;
                }
                taskIds.add(lineageTask.getId());
            }
        }
        Map<String, DAG> taskDagMap = loadTaskDagByTaskId(taskIds);
        Map<String, Boolean> hasJoinMap = new HashMap<>();
        Map<String, Boolean> hasMergeMap = new HashMap<>();
        Map<String, Boolean> hasAppendMap = new HashMap<>();
        Map<String, Map<String, TableProperties>> mergeTablePropertiesMap = new HashMap<>();
        Map<String, Map<String, String>> fieldNameMappingByNodeId = MapUtils.isEmpty(fieldNameMapping) ? new HashMap<>() : fieldNameMapping;
        Map<String, Map<String, String>> lineageNodeIdByTaskNodeIdByTaskId = new HashMap<>();
        for (Node<?> n : dag.getNodes()) {
            if (!(n instanceof LineageTableNode tableNode)) {
                continue;
            }
            if (MapUtils.isEmpty(tableNode.getTasks())) {
                continue;
            }
            for (LineageTask t : tableNode.getTasks().values()) {
                if (null == t || StringUtils.isBlank(t.getId())) {
                    continue;
                }
                Node<?> taskNode = t.getTaskNode();
                if (null == taskNode || StringUtils.isBlank(taskNode.getId())) {
                    continue;
                }
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
            MergeTableNode mergeNode = null;
            for (Node<?> taskNode : nodeMap.values()) {
                if (taskNode instanceof JoinProcessorNode) {
                    hasJoinMap.put(taskId, true);
                } else if (taskNode instanceof MergeTableNode mergeTableNode) {
                    mergeNode = mergeTableNode;
                    hasMergeMap.put(taskId, true);
                    List<MergeTableProperties> mergeProperties = mergeTableNode.getMergeProperties();
                    mergeProperties.forEach(properties -> {
                        String nodeId = properties.getId();
                        Map<String, TableProperties> mainPropertiesMap = loadRootInfo(nodeId, taskDag, properties);
                        //主表只有一个链路来源
                        mainPropertiesMap.forEach((nId, info) -> info.setTableType(MAIN_TABLE));
                        mergePropertiesMap.putAll(mainPropertiesMap);
                        List<MergeTableProperties> children = properties.getChildren();
                        if (null == children || children.isEmpty()) {
                            return;
                        }
                        children.forEach(child -> {
                            String id = child.getId();
                            mergePropertiesMap.putAll(loadRootInfo(id, taskDag, child));
                        });
                    });
                    if (containsAppendMergeType(mergeTableNode)) {
                        hasAppendMap.put(taskId, true);
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
            for (Node<?> sourceNode : CollectionUtils.emptyIfNull(taskDag.getSources())) {
                if (!(sourceNode instanceof TableNode)) {
                    continue;
                }
                String sourceNodeId = sourceNode.getId();
                if (StringUtils.isBlank(sourceNodeId)) {
                    continue;
                }
                JoinProcessorNode firstJoinNode = findFirstDownstreamJoinNode(taskDag, sourceNodeId);
                if (null != firstJoinNode) {
                    List<String> joinKeys = extractJoinKeyFieldNamesForSource(taskDag, sourceNodeId, firstJoinNode);
                    if (CollectionUtils.isNotEmpty(joinKeys)) {
                        TableProperties props = mergePropertiesMap.computeIfAbsent(sourceNodeId, k -> {
                            TableProperties p = new TableProperties();
                            p.setRootNodeId(sourceNodeId);
                            p.setPreNodeId(sourceNodeId);
                            return p;
                        });
                        List<FieldNameMapping> mappings = new ArrayList<>();
                        for (String originName : joinKeys) {
                            String targetFieldName = pickBestTargetFieldName(targetCandidatesBySourceFieldName.get(originName), originName);
                            if (StringUtils.isBlank(targetFieldName)) {
                                continue;
                            }
                            addFieldNameMapping(mappings, originName, targetFieldName);
                        }
                        props.setJoinKeys(mappings);
                        continue;
                    }
                }
                if (null != mergeNode) {
                    continue;
                }
            }
        });
        List<Node> nodes = dag.getNodes();
        for (Node<?> node : nodes) {
            if (!(node instanceof LineageTableNode)) {
                continue;
            }
            LineageMetadataInstance metadata = ((LineageTableNode) node).getMetadata();
            if (null == metadata) {
                continue;
            }
            Map<String, LineageTask> tasks = ((LineageTableNode) node).getTasks();
            if (MapUtils.isNotEmpty(tasks)) {
                for (LineageTask lineageTask : tasks.values()) {
                    if (null == lineageTask || StringUtils.isBlank(lineageTask.getId())) {
                        continue;
                    }
                    String taskId = lineageTask.getId();
                    Node taskNode = lineageTask.getTaskNode();
                    String nodeId = taskNode.getId();
                    boolean hasJoin = Optional.ofNullable(hasJoinMap.get(taskId)).orElse(false);
                    boolean hasMerge = Optional.ofNullable(hasMergeMap.get(taskId)).orElse(false);
                    boolean hasAppend = Optional.ofNullable(hasAppendMap.get(taskId)).orElse(false);
                    final String nodeType;
                    Map<String, TableProperties> propertiesMap = mergeTablePropertiesMap.get(taskId);
                    if (hasJoin) {
                        nodeType = NODE_ATTR_TYPE_JOIN;
                    } else if (hasMerge) {
                        nodeType = NODE_ATTR_TYPE_MERGE;
                    } else if (hasAppend) {
                        nodeType = NODE_ATTR_TYPE_APPEND;
                    } else {
                        nodeType = NODE_ATTR_TYPE_OTHER;
                    }
                    Map<String, Object> attrs = node.getAttrs();
                    if (null == attrs) {
                        attrs = new HashMap<>();
                        node.setAttrs(attrs);
                    }
                    Optional.ofNullable(propertiesMap.get(nodeId))
                            .ifPresent(properties -> {
                                properties.setNodeType(nodeType);
                                node.getAttrs().put(taskId, properties);
                            });
                }
            }
        }
        return taskDagMap;
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

    private JoinProcessorNode findFirstDownstreamJoinNode(DAG taskDag, String startNodeId) {
        if (null == taskDag || StringUtils.isBlank(startNodeId)) {
            return null;
        }
        Set<String> visited = new HashSet<>();
        Queue<String> queue = new java.util.ArrayDeque<>();
        visited.add(startNodeId);
        queue.add(startNodeId);
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
                if (null == succ || StringUtils.isBlank(succ.getId())) {
                    continue;
                }
                if (!visited.add(succ.getId())) {
                    continue;
                }
                if (succ instanceof JoinProcessorNode) {
                    return (JoinProcessorNode) succ;
                }
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
        return keys.stream().filter(StringUtils::isNotBlank).distinct().collect(Collectors.toList());
    }

    protected Map<String, List<FieldNameMapping>> getUpdateConditionFieldList(Dag lineageDag, Map<String, DAG> taskDagMap, Map<String, Map<String, String>> fieldNameMapping) {
        Map<String, List<FieldNameMapping>> updateConditionFieldList = new HashMap<>();
        if (MapUtils.isEmpty(taskDagMap)) {
            return updateConditionFieldList;
        }
        Map<String, LineageTableNode> lineageTableNodeByTableKey = new HashMap<>();
        Map<String, Map<String, String>> fieldNameMappingByNodeId = MapUtils.isEmpty(fieldNameMapping) ? new HashMap<>() : fieldNameMapping;
        if (null != lineageDag && CollectionUtils.isNotEmpty(lineageDag.getNodes())) {
            for (Node<?> n : lineageDag.getNodes()) {
                if (n instanceof LineageTableNode tableNode) {
                    String tableKey = toTableKey(tableNode.getConnectionId(), tableNode.getTable());
                    if (StringUtils.isNotBlank(tableKey)) {
                        lineageTableNodeByTableKey.putIfAbsent(tableKey, tableNode);
                    }
                }
            }
        }

        taskDagMap.forEach((taskId, taskDag) -> {
            if (null == taskDag || CollectionUtils.isEmpty(taskDag.getNodes())) {
                return;
            }
            String effectiveTaskId = taskId;
            if (null == taskDag.getTaskId()) {
                ObjectId objectId = MongoUtils.toObjectId(taskId);
                if (null != objectId) {
                    taskDag.setTaskId(objectId);
                }
            } else {
                effectiveTaskId = taskDag.getTaskId().toHexString();
            }

            TableNode targetTableNode = taskDag.getTargets().stream()
                    .filter(TableNode.class::isInstance)
                    .map(TableNode.class::cast)
                    .findFirst()
                    .orElse(null);
            if (null == targetTableNode || StringUtils.isBlank(targetTableNode.getId())) {
                return;
            }

            Map<String, Map<String, Field>> fieldsByNodeIdCache = new HashMap<>();
            Map<String, Field> targetFieldMap = getFieldMapForNode(effectiveTaskId, targetTableNode.getId(), fieldsByNodeIdCache);
            if (MapUtils.isEmpty(targetFieldMap)) {
                return;
            }

            Map<String, String> tableNodeIdToTableKey = taskDag.getNodes().stream()
                    .filter(Objects::nonNull)
                    .filter(TableNode.class::isInstance)
                    .map(TableNode.class::cast)
                    .filter(n -> StringUtils.isNotBlank(n.getConnectionId()) && StringUtils.isNotBlank(n.getTableName()) && StringUtils.isNotBlank(n.getId()))
                    .collect(Collectors.toMap(Node::getId, n -> toTableKey(n.getConnectionId(), n.getTableName()), (k1, k2) -> k2));

            String targetTableKey = toTableKey(targetTableNode.getConnectionId(), targetTableNode.getTableName());
            String targetLineageNodeId = Optional.ofNullable(lineageTableNodeByTableKey.get(targetTableKey)).map(Node::getId).orElse(null);
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

            for (Node<?> source : CollectionUtils.emptyIfNull(taskDag.getSources())) {
                if (!(source instanceof TableNode)) {
                    continue;
                }
                TableNode sourceTableNode = (TableNode) source;
                String sourceNodeId = sourceTableNode.getId();
                if (StringUtils.isBlank(sourceNodeId)) {
                    continue;
                }
                String sourceTableKey = tableNodeIdToTableKey.get(sourceNodeId);
                if (StringUtils.isBlank(sourceTableKey)) {
                    continue;
                }

                LineageTableNode lineageSourceTableNode = lineageTableNodeByTableKey.get(sourceTableKey);
                List<String> sourceUpdateFields = tryGetUpdateFieldsFromUpstreamTarget(lineageSourceTableNode, effectiveTaskId, taskDagMap);
                if (CollectionUtils.isEmpty(sourceUpdateFields)) {
                    sourceUpdateFields = getPrimaryOrUniqueKeyFieldsForNodeOrSource(effectiveTaskId, sourceTableNode);
                }
                if (CollectionUtils.isEmpty(sourceUpdateFields)) {
                    continue;
                }

                String sourceLineageNodeId = Optional.ofNullable(lineageSourceTableNode).map(Node::getId).orElse(sourceTableKey);
                List<FieldNameMapping> mappings = updateConditionFieldList.computeIfAbsent(sourceLineageNodeId, k -> new ArrayList<>());
                for (String sourceFieldName : sourceUpdateFields) {
                    if (StringUtils.isBlank(sourceFieldName)) {
                        continue;
                    }
                    String targetFieldName = pickBestTargetFieldName(targetCandidatesBySourceFieldName.get(sourceFieldName), sourceFieldName);
                    if (StringUtils.isBlank(targetFieldName) && targetFieldMap.containsKey(sourceFieldName)) {
                        targetFieldName = sourceFieldName;
                    }
                    if (StringUtils.isBlank(targetFieldName)) {
                        continue;
                    }
                    addFieldNameMapping(mappings, sourceFieldName, targetFieldName);
                }
            }
        });
        return updateConditionFieldList;
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
            Object pos = (taskNode instanceof com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode)
                    ? ((com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode) taskNode).getTaskNodePos()
                    : null;
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
        List<String> sorted = targetCandidates.stream().filter(StringUtils::isNotBlank).distinct().sorted(String::compareTo).collect(Collectors.toList());
        return sorted.isEmpty() ? null : sorted.get(0);
    }

    protected static final class NodeFieldState {
        private final String nodeId;
        private final String fieldName;

        private NodeFieldState(String nodeId, String fieldName) {
            this.nodeId = nodeId;
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
            return Objects.equals(nodeId, that.nodeId) && Objects.equals(fieldName, that.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, fieldName);
        }
    }

    protected Map<String, TableProperties> loadRootInfo(String preNodeId, DAG taskDag, MergeTableProperties mergeTableProperties) {
        Map<String, TableProperties> result = new HashMap<>();
        if (StringUtils.isBlank(preNodeId) || null == taskDag || null == taskDag.getTaskId() || null == mergeTableProperties) {
            return result;
        }
        Set<String> rootNodeIds = findRootNodeIds(taskDag, preNodeId);
        String taskId = taskDag.getTaskId().toHexString();
        Map<String, Map<String, Field>> fieldsByNodeIdCache = new HashMap<>();

        if (rootNodeIds.isEmpty() || (rootNodeIds.size() == 1 && rootNodeIds.contains(preNodeId))) {
            TableProperties isSelf = new TableProperties();
            isSelf.setRootNodeId(preNodeId);
            isSelf.setPreNodeId(preNodeId);
            List<Map<String, String>> joinKeys = mergeTableProperties.getJoinKeys();
            if (CollectionUtils.isNotEmpty(joinKeys)) {
                List<FieldNameMapping> joinKeyMappings = new ArrayList<>();
                for (Map<String, String> joinKey : joinKeys) {
                    if (MapUtils.isEmpty(joinKey)) {
                        continue;
                    }
                    String sourceName = joinKey.get("source");
                    String targetName = joinKey.get("target");
                    if (StringUtils.isBlank(targetName)) {
                        continue;
                    }
                    if (StringUtils.isBlank(sourceName)) {
                        continue;
                    }
                    String originName = getOriginFieldName(taskId, preNodeId, sourceName, fieldsByNodeIdCache);
                    addFieldNameMapping(joinKeyMappings, originName, targetName);
                }
                isSelf.setJoinKeys(joinKeyMappings);
            }
            List<String> arrayKeys = mergeTableProperties.getArrayKeys();
            if (CollectionUtils.isNotEmpty(arrayKeys)) {
                List<FieldNameMapping> tablePkMappings = new ArrayList<>();
                for (String targetName : arrayKeys) {
                    if (StringUtils.isBlank(targetName)) {
                        continue;
                    }
                    String originName = getOriginFieldName(taskId, preNodeId, targetName, fieldsByNodeIdCache);
                    addFieldNameMapping(tablePkMappings, originName, targetName);
                }
                isSelf.setTablePk(tablePkMappings);
            }
            result.put(preNodeId, isSelf);
            return result;
        }

        List<String> arrayKeys = mergeTableProperties.getArrayKeys();
        if (CollectionUtils.isNotEmpty(arrayKeys)) {
            for (String arrayKey : arrayKeys) {
                if (StringUtils.isBlank(arrayKey)) {
                    continue;
                }
                Optional<TracedField> traced = traceToRoot(taskDag, taskId, preNodeId, arrayKey, fieldsByNodeIdCache);
                traced.ifPresent(tf -> {
                    TableProperties tableProperties = result.computeIfAbsent(tf.rootNodeId, k -> newTableProperties(tf.rootNodeId, preNodeId));
                    if (null == tableProperties.getTablePk()) {
                        tableProperties.setTablePk(new ArrayList<>());
                    }
                    addFieldNameMapping(tableProperties.getTablePk(), tf.rootFieldName, arrayKey);
                });
            }
        }

        List<Map<String, String>> joinKeys = mergeTableProperties.getJoinKeys();
        if (CollectionUtils.isNotEmpty(joinKeys)) {
            for (Map<String, String> joinKey : joinKeys) {
                if (MapUtils.isEmpty(joinKey)) {
                    continue;
                }
                String sourceName = joinKey.get("source");
                String targetName = joinKey.get("target");
                if (StringUtils.isBlank(sourceName) || StringUtils.isBlank(targetName)) {
                    continue;
                }
                Optional<TracedField> traced = traceToRoot(taskDag, taskId, preNodeId, sourceName, fieldsByNodeIdCache);
                traced.ifPresent(tf -> {
                    TableProperties tableProperties = result.computeIfAbsent(tf.rootNodeId, k -> newTableProperties(tf.rootNodeId, preNodeId));
                    if (null == tableProperties.getJoinKeys()) {
                        tableProperties.setJoinKeys(new ArrayList<>());
                    }
                    addFieldNameMapping(tableProperties.getJoinKeys(), tf.rootFieldName, targetName);
                });
            }
        }
        return result;
    }

    protected Map<String, Map<String, String>> groupFieldNameMappingByNodeId(List<Node> nodes) {
        Map<String, Map<String, String>> mapMap = new HashMap<>();
        if (CollectionUtils.isEmpty(nodes)) {
            return mapMap;
        }
        Set<String> taskIds = new HashSet<>();
        List<LineageTableNode> lineageNodes = new ArrayList<>();
        for (Node<?> node : nodes) {
            if (!(node instanceof LineageTableNode)) {
                continue;
            }
            LineageTableNode tableNode = (LineageTableNode) node;
            lineageNodes.add(tableNode);
            Map<String, LineageTask> tasks = tableNode.getTasks();
            if (MapUtils.isEmpty(tasks)) {
                continue;
            }
            for (LineageTask lineageTask : tasks.values()) {
                if (null == lineageTask || StringUtils.isBlank(lineageTask.getId())) {
                    continue;
                }
                taskIds.add(lineageTask.getId());
            }
        }
        Map<String, DAG> taskDagMap = loadTaskDagByTaskId(taskIds);
        Map<String, Map<String, Map<String, Field>>> fieldsCacheByTaskId = new HashMap<>();

        for (LineageTableNode node : lineageNodes) {
            if (null == node || StringUtils.isBlank(node.getId())) {
                continue;
            }
            Map<String, String> fieldNameMapping = new HashMap<>();
            LineageMetadataInstance metadata = node.getMetadata();
            if (null != metadata && "SOURCE".equalsIgnoreCase(metadata.getSourceType())) {
                ProducingTaskInfo producingTaskInfo = findProducingTaskInfo(node);
                if (null != producingTaskInfo && StringUtils.isNotBlank(producingTaskInfo.taskId) && StringUtils.isNotBlank(producingTaskInfo.nodeId)) {
                    DAG taskDag = taskDagMap.get(producingTaskInfo.taskId);
                    if (null != taskDag) {
                        Map<String, Map<String, Field>> fieldsByNodeIdCache = fieldsCacheByTaskId.computeIfAbsent(producingTaskInfo.taskId, k -> new HashMap<>());
                        String resolvedNodeId = producingTaskInfo.nodeId;
                        Map<String, Field> taskFieldMap = getFieldMapForNode(producingTaskInfo.taskId, resolvedNodeId, fieldsByNodeIdCache);
                        if (MapUtils.isEmpty(taskFieldMap)) {
                            String candidateNodeId = findTableNodeIdInTaskDag(taskDag, node.getConnectionId(), node.getTable(), true);
                            if (StringUtils.isNotBlank(candidateNodeId)) {
                                resolvedNodeId = candidateNodeId;
                                taskFieldMap = getFieldMapForNode(producingTaskInfo.taskId, resolvedNodeId, fieldsByNodeIdCache);
                            }
                        }
                        if (MapUtils.isNotEmpty(taskFieldMap)) {
                            for (String targetFieldName : taskFieldMap.keySet()) {
                                if (StringUtils.isBlank(targetFieldName)) {
                                    continue;
                                }
                                String originName = resolveTaskInternalOriginFieldName(taskDag, producingTaskInfo.taskId, resolvedNodeId, targetFieldName, fieldsByNodeIdCache);
                                fieldNameMapping.putIfAbsent(targetFieldName, StringUtils.defaultString(originName));
                            }
                        }
                    }
                }
                if (fieldNameMapping.isEmpty()) {
                    List<String> fieldNames = new ArrayList<>();
                    List<Field> fields = metadata.getFields();
                    if (CollectionUtils.isNotEmpty(fields)) {
                        for (Field field : fields) {
                            if (null == field || field.isDeleted() || StringUtils.isBlank(field.getFieldName())) {
                                continue;
                            }
                            fieldNames.add(field.getFieldName());
                        }
                    }
                    if (fieldNames.isEmpty()) {
                        fieldNames.addAll(getAllFieldNamesForSource(node.getConnectionId(), node.getTable()));
                    }
                    for (String fieldName : fieldNames) {
                        if (StringUtils.isBlank(fieldName)) {
                            continue;
                        }
                        fieldNameMapping.putIfAbsent(fieldName, "");
                    }
                }
                metadata.setFields(null);
                mapMap.put(node.getId(), fieldNameMapping);
                continue;
            }

            ProducingTaskInfo producingTaskInfo = findProducingTaskInfo(node);
            if (null != producingTaskInfo && StringUtils.isNotBlank(producingTaskInfo.taskId) && StringUtils.isNotBlank(producingTaskInfo.nodeId)) {
                DAG taskDag = taskDagMap.get(producingTaskInfo.taskId);
                if (null != taskDag) {
                    Map<String, Map<String, Field>> fieldsByNodeIdCache = fieldsCacheByTaskId.computeIfAbsent(producingTaskInfo.taskId, k -> new HashMap<>());
                    String resolvedNodeId = producingTaskInfo.nodeId;
                    Map<String, Field> taskFieldMap = getFieldMapForNode(producingTaskInfo.taskId, resolvedNodeId, fieldsByNodeIdCache);
                    if (MapUtils.isEmpty(taskFieldMap)) {
                        String candidateNodeId = findTableNodeIdInTaskDag(taskDag, node.getConnectionId(), node.getTable(), true);
                        if (StringUtils.isNotBlank(candidateNodeId)) {
                            resolvedNodeId = candidateNodeId;
                            taskFieldMap = getFieldMapForNode(producingTaskInfo.taskId, resolvedNodeId, fieldsByNodeIdCache);
                        }
                    }
                    if (MapUtils.isNotEmpty(taskFieldMap)) {
                        for (String targetFieldName : taskFieldMap.keySet()) {
                            if (StringUtils.isBlank(targetFieldName)) {
                                continue;
                            }
                            String originName = resolveTaskInternalOriginFieldName(taskDag, producingTaskInfo.taskId, resolvedNodeId, targetFieldName, fieldsByNodeIdCache);
                            fieldNameMapping.putIfAbsent(targetFieldName, StringUtils.defaultIfBlank(originName, targetFieldName));
                        }
                    }
                }
            }
            if (fieldNameMapping.isEmpty() && null != metadata && CollectionUtils.isNotEmpty(metadata.getFields())) {
                for (Field field : metadata.getFields()) {
                    if (null == field || field.isDeleted() || StringUtils.isBlank(field.getFieldName())) {
                        continue;
                    }
                    String originName = StringUtils.defaultIfBlank(field.getPreviousFieldName(), field.getFieldName());
                    fieldNameMapping.putIfAbsent(field.getFieldName(), originName);
                }
                metadata.setFields(null);
            }
            mapMap.put(node.getId(), fieldNameMapping);
        }
        for (Node<?> node : nodes) {
            if (null == node || StringUtils.isBlank(node.getId())) {
                continue;
            }
            mapMap.putIfAbsent(node.getId(), new HashMap<>());
        }
        return mapMap;
    }

    protected List<String> getAllFieldNamesForSource(String connectionId, String tableName) {
        if (StringUtils.isBlank(connectionId) || StringUtils.isBlank(tableName)) {
            return new ArrayList<>();
        }
        Criteria criteria = Criteria.where("source._id").is(connectionId)
                .and("original_name").is(tableName)
                .and("sourceType").is("SOURCE")
                .and("is_deleted").ne(true);
        Query query = Query.query(criteria);
        query.fields().include("fields");
        MetadataInstancesEntity entity = metadataInstancesRepository.findOne(query).orElse(null);
        List<Field> fields = null == entity ? null : entity.getFields();
        if (CollectionUtils.isEmpty(fields)) {
            return new ArrayList<>();
        }
        List<String> names = new ArrayList<>();
        for (Field field : fields) {
            if (null == field || field.isDeleted() || StringUtils.isBlank(field.getFieldName())) {
                continue;
            }
            names.add(field.getFieldName());
        }
        return names;
    }

    protected String findTableNodeIdInTaskDag(DAG taskDag, String connectionId, String tableName, boolean preferTarget) {
        if (null == taskDag || StringUtils.isBlank(connectionId) || StringUtils.isBlank(tableName)) {
            return null;
        }
        if (preferTarget && CollectionUtils.isNotEmpty(taskDag.getTargets())) {
            for (Node<?> n : taskDag.getTargets()) {
                if (!(n instanceof TableNode)) {
                    continue;
                }
                TableNode t = (TableNode) n;
                if (connectionId.equals(t.getConnectionId()) && tableName.equals(t.getTableName()) && StringUtils.isNotBlank(t.getId())) {
                    return t.getId();
                }
            }
        }
        if (CollectionUtils.isEmpty(taskDag.getNodes())) {
            return null;
        }
        for (Node<?> n : taskDag.getNodes()) {
            if (!(n instanceof TableNode)) {
                continue;
            }
            TableNode t = (TableNode) n;
            if (connectionId.equals(t.getConnectionId()) && tableName.equals(t.getTableName()) && StringUtils.isNotBlank(t.getId())) {
                return t.getId();
            }
        }
        return null;
    }

    protected String resolveTaskInternalOriginFieldName(
            DAG taskDag,
            String taskId,
            String nodeId,
            String targetFieldName,
            Map<String, Map<String, Field>> fieldsByNodeIdCache
    ) {
        if (null == taskDag || StringUtils.isBlank(taskId) || StringUtils.isBlank(nodeId) || StringUtils.isBlank(targetFieldName)) {
            return null;
        }
        Optional<TracedField> traced = traceToRoot(taskDag, taskId, nodeId, targetFieldName, fieldsByNodeIdCache);
        return traced.map(TracedField::getRootFieldName).orElse(null);
    }

    protected ProducingTaskInfo findProducingTaskInfo(LineageTableNode lineageTableNode) {
        if (null == lineageTableNode) {
            return null;
        }
        LineageMetadataInstance metadata = lineageTableNode.getMetadata();
        String metadataNodeId = null == metadata ? null : metadata.getNodeId();
        Map<String, LineageTask> tasks = lineageTableNode.getTasks();
        if (MapUtils.isEmpty(tasks)) {
            return null;
        }
        for (LineageTask lineageTask : tasks.values()) {
            if (null == lineageTask || StringUtils.isBlank(lineageTask.getId()) || null == lineageTask.getTaskNode()) {
                continue;
            }
            Node<?> taskNode = lineageTask.getTaskNode();
            if (!(taskNode instanceof com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode)) {
                continue;
            }
            String pos = ((com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode) taskNode).getTaskNodePos();
            if (!StringUtils.equalsIgnoreCase(pos, com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode.TASK_NODE_TARGET_POS)) {
                continue;
            }
            String nodeId = StringUtils.isNotBlank(metadataNodeId) ? metadataNodeId : taskNode.getId();
            return new ProducingTaskInfo(lineageTask.getId(), nodeId);
        }
        return null;
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

    protected String getOriginFieldName(String taskId, String nodeId, String targetName, Map<String, Map<String, Field>> fieldsByNodeIdCache) {
        Field field = getField(taskId, nodeId, targetName, fieldsByNodeIdCache);
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
        Query query = Query.query(Criteria.where("taskId").is(taskId).and("nodeId").is(nodeId).and("is_deleted").ne(true));
        query.fields().include("fields", "indices");
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
        List<TableIndex> indices = entity.getIndices();
        if (CollectionUtils.isEmpty(indices)) {
            return new ArrayList<>();
        }
        for (TableIndex index : indices) {
            if (null == index || !index.isUnique()) {
                continue;
            }
            List<TableIndexColumn> columns = index.getColumns();
            if (CollectionUtils.isEmpty(columns)) {
                continue;
            }
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

    protected List<String> getPrimaryOrUniqueKeyFieldsForNodeOrSource(String taskId, TableNode tableNode) {
        if (null == tableNode) {
            return new ArrayList<>();
        }
        String nodeId = tableNode.getId();
        List<String> fields = getPrimaryOrUniqueKeyFields(taskId, nodeId);
        if (CollectionUtils.isNotEmpty(fields)) {
            return fields;
        }
        return getPrimaryOrUniqueKeyFieldsFromSource(tableNode.getConnectionId(), tableNode.getTableName());
    }

    protected List<String> getPrimaryOrUniqueKeyFieldsFromSource(String connectionId, String tableName) {
        if (StringUtils.isBlank(connectionId) || StringUtils.isBlank(tableName)) {
            return new ArrayList<>();
        }
        Criteria criteria = Criteria.where("source._id").is(connectionId)
                .and("original_name").is(tableName)
                .and("sourceType").is("SOURCE")
                .and("is_deleted").ne(true);
        Query query = Query.query(criteria);
        query.fields().include("fields", "indices");
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
            if (null == index || !index.isUnique()) {
                continue;
            }
            List<TableIndexColumn> columns = index.getColumns();
            if (CollectionUtils.isEmpty(columns)) {
                continue;
            }
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
            String fieldName,
            Map<String, Map<String, Field>> fieldsByNodeIdCache
    ) {
        if (null == taskDag || StringUtils.isBlank(taskId) || StringUtils.isBlank(startNodeId) || StringUtils.isBlank(fieldName)) {
            return Optional.empty();
        }
        String currentNodeId = startNodeId;
        String currentFieldName = fieldName;
        Set<String> visitedNodeIds = new HashSet<>();
        for (int step = 0; step < 64; step++) {
            if (!visitedNodeIds.add(currentNodeId)) {
                return Optional.empty();
            }
            Field field = getField(taskId, currentNodeId, currentFieldName, fieldsByNodeIdCache);
            if (null == field) {
                return Optional.empty();
            }
            List<Node> predecessors = taskDag.predecessors(currentNodeId);
            if (CollectionUtils.isEmpty(predecessors)) {
                return Optional.of(new TracedField(currentNodeId, currentFieldName));
            }
            String previousFieldName = field.getPreviousFieldName();
            NodeFieldState next = null;
            if (StringUtils.isNotBlank(previousFieldName)) {
                List<NodeFieldState> candidates = new ArrayList<>();
                for (Node<?> predecessor : predecessors) {
                    if (null == predecessor || StringUtils.isBlank(predecessor.getId())) {
                        continue;
                    }
                    Field preField = getField(taskId, predecessor.getId(), previousFieldName, fieldsByNodeIdCache);
                    if (null != preField) {
                        candidates.add(new NodeFieldState(predecessor.getId(), previousFieldName));
                    }
                }
                if (!candidates.isEmpty()) {
                    if (candidates.size() == 1) {
                        next = candidates.get(0);
                    } else {
                        NodeFieldState matched = null;
                        for (NodeFieldState candidate : candidates) {
                            Field preField = getField(taskId, candidate.nodeId, previousFieldName, fieldsByNodeIdCache);
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
                NodeFieldState matched = null;
                for (Node<?> predecessor : predecessors) {
                    if (null == predecessor || StringUtils.isBlank(predecessor.getId())) {
                        continue;
                    }
                    Map<String, Field> preFieldMap = getFieldMapForNode(taskId, predecessor.getId(), fieldsByNodeIdCache);
                    if (MapUtils.isEmpty(preFieldMap)) {
                        continue;
                    }
                    for (Field preField : preFieldMap.values()) {
                        if (null == preField || StringUtils.isBlank(preField.getFieldName())) {
                            continue;
                        }
                        if (isSameFieldLineage(field, preField)) {
                            if (matched != null) {
                                return Optional.empty();
                            }
                            matched = new NodeFieldState(predecessor.getId(), preField.getFieldName());
                        }
                    }
                }
                next = matched;
            }
            if (next == null) {
                return Optional.empty();
            }
            currentNodeId = next.nodeId;
            currentFieldName = next.fieldName;
        }
        return Optional.empty();
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

    protected Map<String, Field> getFieldMapForNode(String taskId, String nodeId, Map<String, Map<String, Field>> fieldsByNodeIdCache) {
        if (StringUtils.isBlank(taskId) || StringUtils.isBlank(nodeId)) {
            return new HashMap<>();
        }
        return fieldsByNodeIdCache.computeIfAbsent(nodeId, nid -> {
            Query query = Query.query(Criteria.where("taskId").is(taskId).and("nodeId").is(nid).and("is_deleted").ne(true));
            query.fields().include("fields");
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

    protected Field getField(String taskId, String nodeId, String fieldName, Map<String, Map<String, Field>> fieldsByNodeIdCache) {
        if (StringUtils.isBlank(taskId) || StringUtils.isBlank(nodeId) || StringUtils.isBlank(fieldName)) {
            return null;
        }
        Map<String, Field> fieldMap = getFieldMapForNode(taskId, nodeId, fieldsByNodeIdCache);
        return fieldMap.get(fieldName);
    }

    @Data
    public static class TableProperties {
        String rootNodeId;
        String preNodeId;
        String nodeType;
        List<FieldNameMapping> joinKeys;
        List<FieldNameMapping> tablePk;
        String tableType = SUB_TABLE;
    }

    @Data
    public static class FieldNameMapping {
        String originName;
        String targetName;
    }

    @Data
    public static class TracedField {
        private final String rootNodeId;
        private final String rootFieldName;
    }

    @Data
    private static class ProducingTaskInfo {
        private final String taskId;
        private final String nodeId;

        private ProducingTaskInfo(String taskId, String nodeId) {
            this.taskId = taskId;
            this.nodeId = nodeId;
        }
    }
}
