package com.tapdata.tm.trace.service.bloodline;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.analyzer.entity.LineageMetadataInstance;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTask;
import com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode;
import com.tapdata.tm.trace.dto.boodline.FieldNameMapping;
import io.github.openlg.graphlib.Graph;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UpdateConditionFieldLoaderTest {

    @Test
    void getUpdateConditionFieldList_shouldReturnEmpty_whenInvalidInputsOrNoTargets() {
        assertEquals(Map.of(), UpdateConditionFieldLoader.getUpdateConditionFieldList(null, Map.of(), Map.of()));

        Dag dag = new Dag();
        dag.setNodes(new ArrayList<>());
        assertEquals(Map.of(), UpdateConditionFieldLoader.getUpdateConditionFieldList(dag, Map.of("t", mock(DAG.class)), Map.of()));

        dag.setNodes(List.of(new LineageTaskNode("x", "n", "t")));
        assertEquals(Map.of(), UpdateConditionFieldLoader.getUpdateConditionFieldList(dag, Map.of("t", mock(DAG.class)), Map.of()));

        LineageTableNode nonTarget = lineageTableNode("c", "t", "n1", false);
        dag.setNodes(List.of(nonTarget));
        assertEquals(Map.of(), UpdateConditionFieldLoader.getUpdateConditionFieldList(dag, Map.of("t", mock(DAG.class)), Map.of()));
    }

    @Test
    void findAllTargetNodes_shouldReturnOnlyTargetNodes() {
        Dag dag = new Dag();
        LineageTableNode target = lineageTableNode("c", "t", "target", true);
        LineageTableNode notTarget = lineageTableNode("c", "t2", "n", false);
        dag.setNodes(List.of(target, notTarget, new LineageTaskNode("x", "n", "t")));

        List<LineageTableNode> result = UpdateConditionFieldLoader.findAllTargetNodes(dag);
        assertEquals(1, result.size());
        assertEquals("target", result.get(0).getId());
    }

    @Test
    void getUpdateConditionFieldList_shouldUseTaskNodeIdMappingFirst_thenFallbackToLineageNodeIdMapping_andDeduplicate() {
        String taskId = "task1";

        LineageTaskNode targetPosNode = new LineageTaskNode("taskNode1", "n", "t");
        targetPosNode.setTaskNodePos(LineageTaskNode.TASK_NODE_TARGET_POS);
        LineageTask task = new LineageTask(taskId, "n", targetPosNode, null, null, null);

        LineageTableNode targetLineage = lineageTableNode("c", "t", "ln1", true);
        targetLineage.setTasks(Map.of(taskId, task));

        Dag lineageDag = new Dag();
        lineageDag.setNodes(List.of(targetLineage));

        TableNode tableTarget = new TableNode();
        tableTarget.setId("tgt");
        tableTarget.setConnectionId("c");
        tableTarget.setTableName("t");
        tableTarget.setUpdateConditionFields(List.of("u1", " ", "u2", "u1"));

        Graph<Node, com.tapdata.tm.commons.dag.Edge> graph = new Graph<>();
        graph.setNode("tgt", tableTarget);
        DAG taskDag = new DAG(graph);
        Map<String, DAG> taskDagMap = Map.of(taskId, taskDag);

        Map<String, Map<String, String>> fieldNameMapping = new HashMap<>();
        fieldNameMapping.put("taskNode1", Map.of("u1", "o1"));
        fieldNameMapping.put("ln1", Map.of("u1", "o1_ln", "u2", ""));

        Map<String, List<FieldNameMapping>> result = UpdateConditionFieldLoader.getUpdateConditionFieldList(lineageDag, taskDagMap, fieldNameMapping);

        assertEquals(1, result.size());
        List<FieldNameMapping> list = result.get("ln1");
        assertNotNull(list);
        assertEquals(2, list.size());
        assertTrue(list.stream().anyMatch(m -> "o1".equals(m.getOriginName()) && "u1".equals(m.getTargetName())));
        assertTrue(list.stream().anyMatch(m -> "u2".equals(m.getOriginName()) && "u2".equals(m.getTargetName())));
    }

    @Test
    void getUpdateConditionFieldList_shouldSupportDatabaseNodeUpdateConditionFieldMap() {
        String taskId = "taskDb";
        String tableName = "t";
        String connectionId = "c";

        LineageTaskNode targetPosNode = new LineageTaskNode("taskNode", "n", "t");
        targetPosNode.setTaskNodePos(LineageTaskNode.TASK_NODE_TARGET_POS);
        LineageTask task = new LineageTask(taskId, "n", targetPosNode, null, null, null);

        LineageTableNode targetLineage = lineageTableNode(connectionId, tableName, "lnDb", true);
        targetLineage.setTasks(Map.of(taskId, task));

        Dag lineageDag = new Dag();
        lineageDag.setNodes(List.of(targetLineage));

        DatabaseNode dbTarget = new DatabaseNode();
        dbTarget.setId("db");
        dbTarget.setConnectionId(connectionId);
        dbTarget.setUpdateConditionFieldMap(Map.of(tableName, List.of("uDb")));

        Graph<Node, com.tapdata.tm.commons.dag.Edge> graph = new Graph<>();
        graph.setNode("db", dbTarget);
        DAG taskDag = new DAG(graph);
        Map<String, DAG> taskDagMap = Map.of(taskId, taskDag);

        Map<String, Map<String, String>> mapping = Map.of("lnDb", Map.of("uDb", "oDb"));
        Map<String, List<FieldNameMapping>> result = UpdateConditionFieldLoader.getUpdateConditionFieldList(lineageDag, taskDagMap, mapping);

        assertEquals(1, result.size());
        assertEquals(1, result.get("lnDb").size());
        assertEquals("oDb", result.get("lnDb").get(0).getOriginName());
        assertEquals("uDb", result.get("lnDb").get(0).getTargetName());
    }

    @Test
    void getUpdateConditionFieldList_shouldSkipWhenTaskDagMissingOrNonTargetPosOrMissingIds() {
        LineageTableNode targetLineage = lineageTableNode("c", "t", "ln", true);

        LineageTaskNode sourcePosNode = new LineageTaskNode("taskNode", "n", "t");
        sourcePosNode.setTaskNodePos(LineageTaskNode.TASK_NODE_SOURCE_POS);

        targetLineage.setTasks(Map.of(
                "blank", new LineageTask(" ", "n", sourcePosNode, null, null, null),
                "sourcePos", new LineageTask("task1", "n", sourcePosNode, null, null, null),
                "missingDag", new LineageTask("task2", "n", targetPos("tn"), null, null, null)
        ));

        Dag lineageDag = new Dag();
        lineageDag.setNodes(List.of(targetLineage));

        Map<String, List<FieldNameMapping>> result = UpdateConditionFieldLoader.getUpdateConditionFieldList(lineageDag, Map.of(), Map.of());
        assertEquals(0, result.size());

        Map<String, List<FieldNameMapping>> result2 = UpdateConditionFieldLoader.getUpdateConditionFieldList(lineageDag, Map.of("task2", mock(DAG.class)), Map.of());
        assertEquals(0, result2.size());
    }

    private static LineageTaskNode targetPos(String id) {
        LineageTaskNode n = new LineageTaskNode(id, "n", "t");
        n.setTaskNodePos(LineageTaskNode.TASK_NODE_TARGET_POS);
        return n;
    }

    private static LineageTableNode lineageTableNode(String connectionId, String table, String id, boolean makeTarget) {
        LineageMetadataInstance metadata = new LineageMetadataInstance();
        metadata.setSourceType("SOURCE");
        LineageTableNode node = new LineageTableNode(table, connectionId, "cn", "pdk", metadata);
        node.setId(id);
        if (makeTarget) {
            LineageTaskNode taskNode = new LineageTaskNode("t", "n", "t");
            taskNode.setTaskNodePos(LineageTaskNode.TASK_NODE_TARGET_POS);
            node.setTasks(Map.of("tid", new LineageTask("tid", "n", taskNode, null, null, null)));
        } else {
            LineageTaskNode taskNode = new LineageTaskNode("s", "n", "t");
            taskNode.setTaskNodePos(LineageTaskNode.TASK_NODE_SOURCE_POS);
            node.setTasks(Map.of("sid", new LineageTask("sid", "n", taskNode, null, null, null)));
        }
        return node;
    }
}

