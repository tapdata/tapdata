package com.tapdata.tm.trace.service.bloodline;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTask;
import com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/26 17:25 Create
 * @description
 */
public class UpdateConditionFieldLoader {

    public static Map<String, List<BloodlineFinder.FieldNameMapping>> getUpdateConditionFieldList(Dag dag, Map<String, DAG> taskDagMap, Map<String, Map<String, String>> fieldNameMapping) {
        Map<String, List<BloodlineFinder.FieldNameMapping>> result = new HashMap<>();
        if (null == dag || CollectionUtils.isEmpty(dag.getNodes()) || MapUtils.isEmpty(taskDagMap)) {
            return result;
        }
        Map<String, Map<String, String>> fieldNameMappingByNodeId = MapUtils.isEmpty(fieldNameMapping) ? new HashMap<>() : fieldNameMapping;
        List<LineageTableNode> targetNodes = findAllTargetNodes(dag);
        if (CollectionUtils.isEmpty(targetNodes)) {
            return result;
        }
        for (LineageTableNode targetNode : targetNodes) {
            String lineageNodeId = targetNode.getId();
            if (StringUtils.isBlank(lineageNodeId) || MapUtils.isEmpty(targetNode.getTasks())) {
                continue;
            }
            for (LineageTask lineageTask : targetNode.getTasks().values()) {
                if (null == lineageTask || StringUtils.isBlank(lineageTask.getId()) || !isTaskNodeTargetPos(lineageTask.getTaskNode())) {
                    continue;
                }
                DAG taskDag = taskDagMap.get(lineageTask.getId());
                if (null == taskDag) {
                    continue;
                }
                List<String> updateFields = resolveUpdateConditionFieldsForTargetTable(taskDag, targetNode);
                if (CollectionUtils.isEmpty(updateFields)) {
                    continue;
                }
                String lineageTaskNodeId = Optional.ofNullable(lineageTask.getTaskNode()).map(Node::getId).orElse(null);
                Map<String, String> nodeFieldMapping = StringUtils.isNotBlank(lineageTaskNodeId)
                        ? fieldNameMappingByNodeId.getOrDefault(lineageTaskNodeId, new HashMap<>())
                        : new HashMap<>();
                if (MapUtils.isEmpty(nodeFieldMapping)) {
                    nodeFieldMapping = fieldNameMappingByNodeId.getOrDefault(lineageNodeId, new HashMap<>());
                }
                List<BloodlineFinder.FieldNameMapping> mappings = result.computeIfAbsent(lineageNodeId, k -> new ArrayList<>());
                for (String targetFieldName : updateFields) {
                    if (StringUtils.isBlank(targetFieldName)) {
                        continue;
                    }
                    String originFieldName = nodeFieldMapping.get(targetFieldName);
                    if (StringUtils.isBlank(originFieldName)) {
                        originFieldName = targetFieldName;
                    }
                    addFieldNameMapping(mappings, originFieldName, targetFieldName);
                }
            }
        }
        return result;
    }

    public static List<LineageTableNode> findAllTargetNodes(Dag dag) {
        List<LineageTableNode> targetNodes = new ArrayList<>();
        if (null == dag || CollectionUtils.isEmpty(dag.getNodes())) {
            return targetNodes;
        }
        for (Node<?> node : dag.getNodes()) {
            if (!(node instanceof LineageTableNode tableNode)
                    || StringUtils.isBlank(tableNode.getId())
                    || MapUtils.isEmpty(tableNode.getTasks())) {
                continue;
            }
            boolean isTarget = tableNode.getTasks().values()
                    .stream()
                    .filter(Objects::nonNull)
                    .anyMatch(t -> StringUtils.isNotBlank(t.getId()) && isTaskNodeTargetPos(t.getTaskNode()));
            if (isTarget) {
                targetNodes.add(tableNode);
            }
        }
        return targetNodes;
    }

    private static boolean isTaskNodeTargetPos(Node<?> taskNode) {
        if (!(taskNode instanceof LineageTaskNode)) {
            return false;
        }
        return StringUtils.equalsIgnoreCase(((LineageTaskNode) taskNode).getTaskNodePos(), LineageTaskNode.TASK_NODE_TARGET_POS);
    }

    private static List<String> resolveUpdateConditionFieldsForTargetTable(DAG taskDag, LineageTableNode targetTableNode) {
        if (null == taskDag || null == targetTableNode) {
            return new ArrayList<>();
        }
        String connectionId = targetTableNode.getConnectionId();
        String tableName = targetTableNode.getTable();
        if (StringUtils.isBlank(connectionId) || StringUtils.isBlank(tableName)) {
            return new ArrayList<>();
        }
        for (Node<?> target : CollectionUtils.emptyIfNull(taskDag.getTargets())) {
            if (target instanceof TableNode tableNode) {
                if (!StringUtils.equals(connectionId, tableNode.getConnectionId())
                        || !StringUtils.equals(tableName, tableNode.getTableName())) {
                    continue;
                }
                return Optional.ofNullable(tableNode.getUpdateConditionFields()).orElseGet(ArrayList::new);
            } else if (target instanceof DatabaseNode databaseNode) {
                if (!StringUtils.equals(connectionId, databaseNode.getConnectionId())) {
                    continue;
                }
                List<String> configured = MapUtils.isEmpty(databaseNode.getUpdateConditionFieldMap())
                        ? null
                        : databaseNode.getUpdateConditionFieldMap().get(tableName);
                if (CollectionUtils.isNotEmpty(configured)) {
                    return configured;
                }
            }
        }
        return new ArrayList<>();
    }

    private static void addFieldNameMapping(List<BloodlineFinder.FieldNameMapping> list, String originName, String targetName) {
        if (null == list || StringUtils.isBlank(originName) || StringUtils.isBlank(targetName)) {
            return;
        }
        boolean exists = list.stream()
                .filter(Objects::nonNull)
                .anyMatch(m -> originName.equals(m.getOriginName()) && targetName.equals(m.getTargetName()));
        if (exists) {
            return;
        }
        BloodlineFinder.FieldNameMapping mapping = new BloodlineFinder.FieldNameMapping();
        mapping.setOriginName(originName);
        mapping.setTargetName(targetName);
        list.add(mapping);
    }
}
