package com.tapdata.tm.trace.service.bloodline;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTask;
import com.tapdata.tm.trace.dto.boodline.FieldNameMapping;
import com.tapdata.tm.trace.dto.boodline.NodeFieldState;
import com.tapdata.tm.trace.dto.boodline.TableProperties;
import com.tapdata.tm.trace.dto.boodline.TracedField;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.util.ArrayList;
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
 * @version v1.0 2026/5/28 12:16 Create
 * @description
 */
@Service
public class JoinStateSetter {
    public static final String MAIN_TABLE = "mainTable";
    public static final String SUB_TABLE = "subTable";
    private static final String NODE_ATTR_TYPE_JOIN = "JOIN";
    private static final String NODE_ATTR_TYPE_MERGE = "MERGE";
    private static final String NODE_ATTR_TYPE_APPEND = "APPEND";
    private static final String NODE_ATTR_TYPE_OTHER = "OTHER";

    @Resource(name = "fieldOriginalNameMapping")
    FieldOriginalNameMapping fieldOriginalNameMapping;

    public void markJoinState(Dag dag, Map<String, Map<String, String>> fieldNameMapping, Map<String, DAG> taskDagMap) {
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
                    TableProperties props = mergePropertiesMap.computeIfAbsent(sourceNodeId, k -> newTableProperties(sourceNodeId, sourceNodeId));
                    List<String> joinKeys = extractJoinKeyFieldNamesForSource(taskDag, sourceNodeId, firstJoinNode);
                    if (CollectionUtils.isNotEmpty(joinKeys)) {
                        List<FieldNameMapping> mappings = new ArrayList<>();
                        Map<String, Map<String, Field>> fieldsByNodeIdCache = new HashMap<>();
                        for (String originName : joinKeys) {
                            String realOriginName = fieldOriginalNameMapping.getOriginFieldName(taskId, firstBeforeJoinNode.getId(), tableName, originName, fieldsByNodeIdCache);
                            String targetFieldName = pickBestTargetFieldName(targetCandidatesBySourceFieldName.get(realOriginName), realOriginName);
                            if (StringUtils.isBlank(targetFieldName)) {
                                targetFieldName = originName;
                            }
                            fieldOriginalNameMapping.addFieldNameMapping(mappings, realOriginName, targetFieldName);
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
            if (null == lineageTask || StringUtils.isBlank(lineageTask.getId()) || null == lineageTask.getTaskNode()) {
                continue;
            }
            String taskId = lineageTask.getId();
            Node<?> taskNode = lineageTask.getTaskNode();
            String nodeId = taskNode.getId();
            assert null != nodeId;
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
            Optional.ofNullable(propertiesMap)
                    .map(m -> m.get(nodeId))
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

    protected String toTableKey(String connectionId, String tableName) {
        if (StringUtils.isBlank(connectionId) || StringUtils.isBlank(tableName)) {
            return null;
        }
        return connectionId + "_" + tableName;
    }

    void collectJoinKeys(List<MergeTableProperties> infos, Map<String, TableProperties> mergePropertiesMap, DAG taskDag, String tableType) {
        if (CollectionUtils.isEmpty(infos)) {
            return;
        }
        infos.forEach(child -> {
            String id = child.getId();
            Map<String, TableProperties> propertiesMap = loadRootInfo(id, taskDag, child);
            propertiesMap.forEach((k, v) -> v.setTableType(tableType));
            mergePropertiesMap.putAll(propertiesMap);
            List<MergeTableProperties> sub = child.getChildren();
            collectChildrenJoinKeys(sub, mergePropertiesMap, taskDag);
        });
    }

    void collectChildrenJoinKeys(List<MergeTableProperties> children, Map<String, TableProperties> mergePropertiesMap, DAG taskDag) {
        if (null == children || children.isEmpty()) {
            return;
        }
        collectJoinKeys(children, mergePropertiesMap, taskDag, JoinStateSetter.SUB_TABLE);
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
            TableProperties isSelf = newTableProperties(rootNodeId, preNodeId);
            isSelf.setPath(resolveMergePropertiesPath(mergeTableProperties));
            List<Map<String, String>> joinKeys = Optional.ofNullable(mergeTableProperties.getJoinKeys())
                    .orElse(new ArrayList<>())
                    .stream()
                    .filter(Objects::nonNull)
                    .filter(MapUtils::isNotEmpty)
                    .toList();
            isSelfSetJoinKey(isSelf, taskId, preNodeId, preTableName, fieldsByNodeIdCache, joinKeys);
            isSelfSetTablePk(isSelf, taskId, preNodeId, preTableName, fieldsByNodeIdCache, mergeTableProperties);
            isSelf.setTableType(JoinStateSetter.SUB_TABLE);
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
            String originName = fieldOriginalNameMapping.getOriginFieldName(taskId, preNodeId, preTableName, sourceName, fieldsByNodeIdCache);
            fieldOriginalNameMapping.addFieldNameMapping(joinKeyMappings, originName, targetName);
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
                String originName = fieldOriginalNameMapping.getOriginFieldName(taskId, preNodeId, preTableName, targetName, fieldsByNodeIdCache);
                fieldOriginalNameMapping.addFieldNameMapping(tablePkMappings, originName, targetName);
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
                TableProperties tableProperties = result.computeIfAbsent(tf.getRootNodeId(), k -> newTableProperties(tf.getRootNodeId(), preNodeId));
                if (null == tableProperties.getTablePk()) {
                    tableProperties.setTablePk(new ArrayList<>());
                }
                if (StringUtils.isBlank(tableProperties.getPath())) {
                    tableProperties.setPath(resolveMergePropertiesPath(mergeTableProperties));
                }
                fieldOriginalNameMapping.addFieldNameMapping(tableProperties.getTablePk(), tf.getRootFieldName(), arrayKey);
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
                TableProperties tableProperties = result.computeIfAbsent(tf.getRootNodeId(), k -> JoinStateSetter.newTableProperties(tf.getRootNodeId(), preNodeId));
                if (null == tableProperties.getJoinKeys()) {
                    tableProperties.setJoinKeys(new ArrayList<>());
                }
                if (StringUtils.isBlank(tableProperties.getPath())) {
                    tableProperties.setPath(resolveMergePropertiesPath(mergeTableProperties));
                }
                fieldOriginalNameMapping.addFieldNameMapping(tableProperties.getJoinKeys(), tf.getRootFieldName(), targetName);
            });
        }
    }

    private String resolveMergePropertiesPath(MergeTableProperties mergeTableProperties) {
        if (null == mergeTableProperties) {
            return null;
        }
        return StringUtils.defaultIfBlank(mergeTableProperties.getArrayPath(), mergeTableProperties.getTargetPath());
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
            Field field = fieldOriginalNameMapping.getField(taskId, currentNodeId, currentTableName, currentFieldName, fieldsByNodeIdCache);
            if (null == field) {
                return Optional.empty();
            }
            List<Node> predecessors = taskDag.predecessors(currentNodeId);
            if (CollectionUtils.isEmpty(predecessors)) {
                return Optional.of(new TracedField(currentNodeId, currentFieldName));
            }
            predecessors = predecessors.stream()
                    .filter(Objects::nonNull)
                    .filter(predecessor -> StringUtils.isNotBlank(predecessor.getId()))
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
                    Field preField = fieldOriginalNameMapping.getField(taskId, predecessor.getId(), preTableName, previousFieldName, fieldsByNodeIdCache);
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
                            Field preField = fieldOriginalNameMapping.getField(taskId, candidate.getNodeId(), candidate.getTableName(), previousFieldName, fieldsByNodeIdCache);
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
            currentNodeId = next.getNodeId();
            currentTableName = next.getTableName();
            currentFieldName = next.getFieldName();
        }
        return Optional.empty();
    }

    NodeFieldState matchedNodeFieldState(String taskId, String currentTableName, List<Node> predecessors, DAG taskDag, Map<String, Map<String, Field>> fieldsByNodeIdCache, Field field) {
        NodeFieldState matched = null;
        for (Node<?> predecessor : predecessors) {
            String preTableName = resolvePredecessorTableName(taskDag, predecessor.getId(), currentTableName);
            Map<String, Field> preFieldMap = fieldOriginalNameMapping.getFieldMapForNode(taskId, predecessor.getId(), preTableName, fieldsByNodeIdCache);
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

    protected String resolvePredecessorTableName(DAG taskDag, String preNodeId, String currentTableName) {
        String resolved = resolveNodeTableName(taskDag, preNodeId);
        if (StringUtils.isNotBlank(resolved)) {
            return resolved;
        }
        return currentTableName;
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

    public static TableProperties newTableProperties(String rootNodeId, String preNodeId) {
        TableProperties tableProperties = new TableProperties();
        tableProperties.setRootNodeId(rootNodeId);
        tableProperties.setPreNodeId(preNodeId);
        return tableProperties;
    }

}
