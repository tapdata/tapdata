package com.tapdata.tm.trace.service.bloodline;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTask;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/28 14:31 Create
 * @description
 */
@Service
public class TableUpdateFieldGetter {
    @Resource(name = "fieldOriginalNameMapping")
    FieldOriginalNameMapping fieldOriginalNameMapping;

    public List<String> getTargetTableUpdateFields(Dag dag, Map<String, DAG> taskDagMap) {
        List<String> targetTableUpdateFields = new ArrayList<>();
        if (null == dag || CollectionUtils.isEmpty(dag.getNodes()) || MapUtils.isEmpty(taskDagMap)) {
            return targetTableUpdateFields;
        }
        LineageTableNode targetLineageTableNode = fieldOriginalNameMapping.findFinalTargetLineageTableNode(dag);
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
                .filter(t -> fieldOriginalNameMapping.isTaskNodeTargetPos(t.getTaskNode()))
                .toList();
        if (CollectionUtils.isNotEmpty(targetPosTasks)) {
            taskCandidates = targetPosTasks;
        }
        eachTaskCandidates(taskCandidates, taskDagMap, targetLineageTableNode, targetTableUpdateFields);
        targetTableUpdateFields = targetTableUpdateFields.stream().filter(StringUtils::isNotBlank).distinct().toList();
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
            Node<?> node = findTargetTableNodeInTaskDag(taskDag, targetLineageTableNode.getConnectionId(), targetLineageTableNode.getTable());
            if (null == node) {
                continue;
            }
            if (node instanceof TableNode targetTableNode) {
                List<String> configured = targetTableNode.getUpdateConditionFields();
                if (CollectionUtils.isNotEmpty(configured)) {
                    targetTableUpdateFields.addAll(configured);
                    return;
                }
            }
            if (node instanceof DatabaseNode databaseNode) {
                Map<String, List<String>> updateConditionFieldMap = Optional.ofNullable(databaseNode.getUpdateConditionFieldMap()).orElse(new HashMap<>());
                List<String> configured = updateConditionFieldMap.get(targetLineageTableNode.getTable());
                if (CollectionUtils.isNotEmpty(configured)) {
                    targetTableUpdateFields.addAll(configured);
                    return;
                }
            }
            List<String> fallback = fieldOriginalNameMapping.getPrimaryOrUniqueKeyFieldsForNodeOrSource(effectiveTaskId, node.getId(), targetLineageTableNode.getConnectionId(), targetLineageTableNode.getTable());
            if (CollectionUtils.isNotEmpty(fallback)) {
                targetTableUpdateFields.addAll(fallback);
                return;
            }
        }
    }

    protected Node<?> findTargetTableNodeInTaskDag(DAG taskDag, String connectionId, String tableName) {
        if (null == taskDag || StringUtils.isBlank(connectionId) || StringUtils.isBlank(tableName)) {
            return null;
        }
        List<Node> targets = taskDag.getTargets();
        Node<?> n = findNode(connectionId, tableName, targets);
        if (null != n) {
            return n;
        }
        for (Node<?> node : targets) {
            if (node instanceof TableNode || node instanceof DatabaseNode) {
                return node;
            }
        }
        return findNode(connectionId, tableName, taskDag.getNodes());
    }

    Node<?> findNode(String connectionId, String tableName, List<Node> targets) {
        if (CollectionUtils.isEmpty(targets)) {
            return null;
        }
        for (Node<?> node : targets) {
            if (node instanceof TableNode tableNode && connectionId.equals(tableNode.getConnectionId()) && tableName.equals(tableNode.getTableName())) {
                return tableNode;
            }
            if (node instanceof DatabaseNode dbNode && connectionId.equals(dbNode.getConnectionId())) {
                List<String> tableNames = dbNode.getTableNames();
                if (CollectionUtils.isNotEmpty(tableNames) && tableNames.contains(tableName)) {
                    return dbNode;
                }
            }
        }
        return null;
    }
}
