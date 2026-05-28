package com.tapdata.tm.trace.service.bloodline;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.analyzer.entity.LineageMetadataInstance;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTask;
import com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TableUpdateFieldGetterTest {
    private FieldOriginalNameMapping fieldOriginalNameMapping;
    private TableUpdateFieldGetter service;

    @BeforeEach
    void setUp() {
        fieldOriginalNameMapping = mock(FieldOriginalNameMapping.class);
        service = new TableUpdateFieldGetter();
        service.fieldOriginalNameMapping = fieldOriginalNameMapping;
    }

    @Test
    void getTargetTableUpdateFields_shouldReturnEmpty_whenDagNullOrEmptyOrTaskDagMapEmpty() {
        assertEquals(List.of(), service.getTargetTableUpdateFields(null, Map.of()));

        Dag dag = new Dag();
        dag.setNodes(new ArrayList<>());
        assertEquals(List.of(), service.getTargetTableUpdateFields(dag, Map.of("t", mock(DAG.class))));
        dag.setNodes(List.of(lineageNode("c", "t", "n")));
        assertEquals(List.of(), service.getTargetTableUpdateFields(dag, Map.of()));
    }

    @Test
    void getTargetTableUpdateFields_shouldReturnEmpty_whenNoFinalTargetOrNoTasks() {
        Dag dag = new Dag();
        dag.setNodes(List.of(lineageNode("c", "t", "n")));

        when(fieldOriginalNameMapping.findFinalTargetLineageTableNode(eq(dag))).thenReturn(null);
        assertEquals(List.of(), service.getTargetTableUpdateFields(dag, Map.of("task", mock(DAG.class))));

        LineageTableNode target = lineageNode("c", "t", "target");
        target.setTasks(new HashMap<>());
        when(fieldOriginalNameMapping.findFinalTargetLineageTableNode(eq(dag))).thenReturn(target);
        assertEquals(List.of(), service.getTargetTableUpdateFields(dag, Map.of("task", mock(DAG.class))));
    }

    @Test
    void getTargetTableUpdateFields_shouldPreferTargetPosTasks_andReturnDistinctNonBlank() {
        Dag dag = new Dag();
        dag.setNodes(List.of(lineageNode("c", "t", "n")));

        LineageTableNode target = lineageNode("c", "t", "target");
        LineageTaskNode posTarget = new LineageTaskNode("tn1", "n", "t");
        posTarget.setTaskNodePos(LineageTaskNode.TASK_NODE_TARGET_POS);
        LineageTaskNode posSource = new LineageTaskNode("tn2", "n", "t");
        posSource.setTaskNodePos(LineageTaskNode.TASK_NODE_SOURCE_POS);

        LineageTask t1 = new LineageTask("task1", "n", posTarget, null, null, null);
        LineageTask t2 = new LineageTask("task2", "n", posSource, null, null, null);
        LineageTask tBlank = new LineageTask(" ", "n", posTarget, null, null, null);
        Map<String, LineageTask> tasks = new HashMap<>();
        tasks.put("null", null);
        tasks.put("t1", t1);
        tasks.put("t2", t2);
        tasks.put("blank", tBlank);
        target.setTasks(tasks);

        when(fieldOriginalNameMapping.findFinalTargetLineageTableNode(eq(dag))).thenReturn(target);
        when(fieldOriginalNameMapping.isTaskNodeTargetPos(eq(posTarget))).thenReturn(true);
        when(fieldOriginalNameMapping.isTaskNodeTargetPos(eq(posSource))).thenReturn(false);

        DAG dag1 = mock(DAG.class);
        when(dag1.getTaskId()).thenReturn(new ObjectId("507f1f77bcf86cd799439011"));
        DAG dag2 = mock(DAG.class);
        when(dag2.getTaskId()).thenReturn(null);
        Map<String, DAG> taskDagMap = new HashMap<>();
        taskDagMap.put("task1", dag1);
        taskDagMap.put("task2", dag2);

        TableUpdateFieldGetter spyGetter = spy(service);

        TableNode taskTarget = new TableNode();
        taskTarget.setId("x");
        taskTarget.setConnectionId("c");
        taskTarget.setTableName("t");
        taskTarget.setUpdateConditionFields(List.of(" ", "u1", "u1", "u2"));
        doReturn(taskTarget).when(spyGetter).findTargetTableNodeInTaskDag(eq(dag1), eq("c"), eq("t"));

        List<String> result = spyGetter.getTargetTableUpdateFields(dag, taskDagMap);
        assertEquals(List.of("u1", "u2"), result);
        verify(spyGetter, times(1)).findTargetTableNodeInTaskDag(eq(dag1), eq("c"), eq("t"));
        verify(spyGetter, never()).findTargetTableNodeInTaskDag(eq(dag2), anyString(), anyString());
    }

    @Test
    void eachTaskCandidates_shouldTakeConfiguredThenFallbackThenSkipMissingDagOrTargetTableNode() {
        LineageTableNode targetLineageTableNode = lineageNode("c", "t", "ln");

        LineageTask t1 = new LineageTask("task1", "n", null, null, null, null);
        LineageTask t2 = new LineageTask("task2", "n", null, null, null, null);
        LineageTask t3 = new LineageTask("task3", "n", null, null, null, null);
        List<LineageTask> candidates = List.of(t1, t2, t3);

        DAG missing = null;
        DAG d1 = mock(DAG.class);
        when(d1.getTaskId()).thenReturn(null);
        DAG d2 = mock(DAG.class);
        when(d2.getTaskId()).thenReturn(new ObjectId("507f1f77bcf86cd799439012"));
        DAG d3 = mock(DAG.class);
        when(d3.getTaskId()).thenReturn(null);

        Map<String, DAG> taskDagMap = new HashMap<>();
        taskDagMap.put("task1", missing);
        taskDagMap.put("task2", d2);
        taskDagMap.put("task3", d3);

        TableUpdateFieldGetter spyGetter = spy(service);

        TableNode targetTableNode = new TableNode();
        targetTableNode.setId("t");
        targetTableNode.setConnectionId("c");
        targetTableNode.setTableName("t");
        targetTableNode.setUpdateConditionFields(List.of("cfg1", " ", "cfg2"));

        doReturn(targetTableNode).when(spyGetter).findTargetTableNodeInTaskDag(eq(d2), eq("c"), eq("t"));

        List<String> out = new ArrayList<>();
        spyGetter.eachTaskCandidates(candidates, taskDagMap, targetLineageTableNode, out);
        assertEquals(List.of("cfg1", " ", "cfg2"), out);
        verify(fieldOriginalNameMapping, never()).getPrimaryOrUniqueKeyFieldsForNodeOrSource(anyString(), any(TableNode.class));

        targetTableNode.setUpdateConditionFields(new ArrayList<>());
        when(fieldOriginalNameMapping.getPrimaryOrUniqueKeyFieldsForNodeOrSource(eq("507f1f77bcf86cd799439012"), eq(targetTableNode))).thenReturn(List.of("pk1"));

        out = new ArrayList<>();
        spyGetter.eachTaskCandidates(candidates, taskDagMap, targetLineageTableNode, out);
        assertEquals(List.of("pk1"), out);

        doReturn(null).when(spyGetter).findTargetTableNodeInTaskDag(eq(d2), eq("c"), eq("t"));
        out = new ArrayList<>();
        spyGetter.eachTaskCandidates(candidates, taskDagMap, targetLineageTableNode, out);
        assertEquals(List.of(), out);
    }

    @Test
    void findTargetTableNodeInTaskDag_shouldCoverAllBranches() {
        DAG dag = mock(DAG.class);
        assertNull(service.findTargetTableNodeInTaskDag(null, "c", "t"));
        assertNull(service.findTargetTableNodeInTaskDag(dag, "", "t"));
        assertNull(service.findTargetTableNodeInTaskDag(dag, "c", " "));

        TableNode match = new TableNode();
        match.setId("m");
        match.setConnectionId("c");
        match.setTableName("t");
        TableNode other = new TableNode();
        other.setId("o");
        other.setConnectionId("c2");
        other.setTableName("t2");

        when(dag.getTargets()).thenReturn(List.of(other, match));
        assertSame(match, service.findTargetTableNodeInTaskDag(dag, "c", "t"));

        when(dag.getTargets()).thenReturn(List.of(other));
        assertSame(other, service.findTargetTableNodeInTaskDag(dag, "c", "t"));

        when(dag.getTargets()).thenReturn(new ArrayList<>());
        when(dag.getNodes()).thenReturn(List.of(other, match));
        assertSame(match, service.findTargetTableNodeInTaskDag(dag, "c", "t"));

        when(dag.getNodes()).thenReturn(List.of(other));
        assertNull(service.findTargetTableNodeInTaskDag(dag, "c", "t"));
    }

    private static LineageTableNode lineageNode(String connectionId, String table, String id) {
        LineageMetadataInstance metadata = new LineageMetadataInstance();
        metadata.setSourceType("SOURCE");
        LineageTableNode n = new LineageTableNode(table, connectionId, "cn", "pdk", metadata);
        n.setId(id);
        return n;
    }
}
