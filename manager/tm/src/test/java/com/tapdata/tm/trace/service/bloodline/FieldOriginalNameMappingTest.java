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
import com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.trace.dto.boodline.FieldNameMapping;
import com.tapdata.tm.trace.dto.boodline.TableProperties;
import io.github.openlg.graphlib.Graph;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FieldOriginalNameMappingTest {
    private MetadataInstancesRepository metadataInstancesRepository;
    private FieldOriginalNameMapping service;

    @BeforeEach
    void setUp() {
        metadataInstancesRepository = mock(MetadataInstancesRepository.class);
        service = new FieldOriginalNameMapping();
        service.metadataInstancesRepository = metadataInstancesRepository;
    }

    @Test
    void groupFieldOriginalNameMappingByNodeId_shouldReturnEmpty_whenNodesEmpty() {
        Map<String, Map<String, String>> result = service.groupFieldOriginalNameMappingByNodeId(null);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    void addFieldNameMapping_shouldIgnoreInvalidAndAvoidDuplicates() {
        List<FieldNameMapping> list = new ArrayList<>();
        list.add(null);
        FieldNameMapping existed = new FieldNameMapping();
        existed.setOriginName("o1");
        existed.setTargetName("t1");
        list.add(existed);

        service.addFieldNameMapping(null, "o2", "t2");
        service.addFieldNameMapping(list, "", "t2");
        service.addFieldNameMapping(list, "o2", " ");
        service.addFieldNameMapping(list, "o1", "t1");
        service.addFieldNameMapping(list, "o2", "t2");

        assertEquals(3, list.size());
        FieldNameMapping added = list.get(2);
        assertEquals("o2", added.getOriginName());
        assertEquals("t2", added.getTargetName());
    }

    @Test
    void getFieldMapForNode_shouldReturnEmpty_whenTaskIdOrNodeIdBlank() {
        Map<String, Map<String, Field>> cache = new HashMap<>();
        assertEquals(0, service.getFieldMapForNode("", "n1", "t1", cache).size());
        assertEquals(0, service.getFieldMapForNode("task", " ", "t1", cache).size());
    }

    @Test
    void getOriginFieldName_shouldReturnOriginalOrFallbackToTargetName() {
        MetadataInstancesEntity entity = new MetadataInstancesEntity();
        entity.setFields(List.of(
                field("f1", "of1", false, false, null),
                field("f2", "", false, false, null)
        ));
        when(metadataInstancesRepository.findOne(any())).thenReturn(Optional.of(entity));

        Map<String, Map<String, Field>> cache = new HashMap<>();
        assertEquals("of1", service.getOriginFieldName("task", "node", "t", "f1", cache));
        assertEquals("f2", service.getOriginFieldName("task", "node", "t", "f2", cache));
        assertEquals("missing", service.getOriginFieldName("task", "node", "t", "missing", cache));
    }

    @Test
    void effectiveTaskId_shouldUseExistingTaskIdOrSetFromStringOrFallback() {
        String validId = "507f1f77bcf86cd799439011";
        DAG dag1 = new DAG(new Graph<>());
        assertEquals(validId, service.effectiveTaskId(dag1, validId));
        assertNotNull(dag1.getTaskId());

        DAG dag2 = new DAG(new Graph<>());
        ObjectId objectId = new ObjectId(validId);
        dag2.setTaskId(objectId);
        assertEquals(validId, service.effectiveTaskId(dag2, "ignored"));

        DAG dag3 = new DAG(new Graph<>());
        assertEquals("invalid", service.effectiveTaskId(dag3, "invalid"));
        assertNull(dag3.getTaskId());
    }

    @Test
    void isTaskNodeTargetPos_shouldReturnExpected() {
        assertFalse(service.isTaskNodeTargetPos(new LineageTableNode("t", "c", "cn", "pdk", null)));

        LineageTaskNode target = new LineageTaskNode("n1", "n", "t");
        target.setTaskNodePos(LineageTaskNode.TASK_NODE_TARGET_POS);
        assertTrue(service.isTaskNodeTargetPos(target));

        LineageTaskNode source = new LineageTaskNode("n2", "n", "t");
        source.setTaskNodePos(LineageTaskNode.TASK_NODE_SOURCE_POS);
        assertFalse(service.isTaskNodeTargetPos(source));
    }

    @Test
    void getPrimaryOrUniqueKeyFieldsFromSource_shouldReturnEmptyOnInvalidOrEntityNullElseExtract() {
        assertEquals(0, service.getPrimaryOrUniqueKeyFieldsFromSource("", "t1").size());
        assertEquals(0, service.getPrimaryOrUniqueKeyFieldsFromSource("c1", " ").size());

        when(metadataInstancesRepository.findOne(any())).thenReturn(Optional.empty());
        assertEquals(0, service.getPrimaryOrUniqueKeyFieldsFromSource("c1", "t1").size());

        MetadataInstancesEntity entity = new MetadataInstancesEntity();
        entity.setFields(List.of(field("pk", "pk", false, true, null)));
        entity.setIndices(new ArrayList<>());
        when(metadataInstancesRepository.findOne(any())).thenReturn(Optional.of(entity));
        assertEquals(List.of("pk"), service.getPrimaryOrUniqueKeyFieldsFromSource("c1", "t1"));
    }

    @Test
    void getPrimaryOrUniqueKeyFieldsForNodeOrSource_shouldPreferNodeThenSourceAndHandleNullTableNode() {
        assertEquals(0, service.getPrimaryOrUniqueKeyFieldsForNodeOrSource("task", (TableNode) null).size());

        FieldOriginalNameMapping spy = spy(service);
        TableNode tableNode = new TableNode();
        tableNode.setId("n1");
        tableNode.setConnectionId("c1");
        tableNode.setTableName("t1");

        doReturn(List.of("nodePk")).when(spy).getPrimaryOrUniqueKeyFields("task", "n1");
        assertEquals(List.of("nodePk"), spy.getPrimaryOrUniqueKeyFieldsForNodeOrSource("task", tableNode));

        doReturn(new ArrayList<>()).when(spy).getPrimaryOrUniqueKeyFields("task", "n1");
        doReturn(List.of("srcPk")).when(spy).getPrimaryOrUniqueKeyFieldsFromSource("c1", "t1");
        assertEquals(List.of("srcPk"), spy.getPrimaryOrUniqueKeyFieldsForNodeOrSource("task", "n1", "c1", "t1"));
    }

    @Test
    void tryGetUpdateFieldsFromUpstreamTarget_shouldCoverAllBranches() {
        FieldOriginalNameMapping spy = spy(service);
        assertEquals(0, spy.tryGetUpdateFieldsFromUpstreamTarget(null, "task", new HashMap<>()).size());

        LineageTableNode lineageTableNode = new LineageTableNode("t", "c", "cn", "pdk", null);
        lineageTableNode.setTasks(new HashMap<>());
        assertEquals(0, spy.tryGetUpdateFieldsFromUpstreamTarget(lineageTableNode, "task", new HashMap<>()).size());

        LineageTaskNode sourceTaskNode = new LineageTaskNode("x1", "n", "t");
        sourceTaskNode.setTaskNodePos(LineageTaskNode.TASK_NODE_SOURCE_POS);
        LineageTask sourcePos = new LineageTask("u1", "n", sourceTaskNode, null, null, null);

        LineageTask blankId = new LineageTask(" ", "n", sourceTaskNode, null, null, null);
        LineageTask current = new LineageTask("current", "n", sourceTaskNode, null, null, null);

        LineageTaskNode targetTaskNode = new LineageTaskNode("x2", "n", "t");
        targetTaskNode.setTaskNodePos(LineageTaskNode.TASK_NODE_TARGET_POS);
        LineageTask targetPosNoDag = new LineageTask("u2", "n", targetTaskNode, null, null, null);

        DAG upstreamNoTarget = new DAG(new Graph<>());
        LineageTask targetPosNoTarget = new LineageTask("u3", "n", targetTaskNode, null, null, null);

        Graph<Node, Edge> upstreamConfiguredGraph = new Graph<>();
        DAG upstreamConfigured = new DAG(upstreamConfiguredGraph);
        TableNode upstreamTarget = new TableNode();
        upstreamTarget.setId("up");
        upstreamTarget.setUpdateConditionFields(List.of("cfg1"));
        upstreamConfiguredGraph.setNode(upstreamTarget.getId(), upstreamTarget);
        LineageTask configured = new LineageTask("u4", "n", targetTaskNode, null, null, null);

        Graph<Node, Edge> upstreamNeedPkGraph = new Graph<>();
        DAG upstreamNeedPk = new DAG(upstreamNeedPkGraph);
        TableNode upstreamTarget2 = new TableNode();
        upstreamTarget2.setId("up2");
        upstreamNeedPkGraph.setNode(upstreamTarget2.getId(), upstreamTarget2);
        LineageTask needPk = new LineageTask("u5", "n", targetTaskNode, null, null, null);

        Map<String, LineageTask> tasks = new HashMap<>();
        tasks.put("blank", blankId);
        tasks.put("current", current);
        tasks.put("source", sourcePos);
        tasks.put("noDag", targetPosNoDag);
        tasks.put("noTarget", targetPosNoTarget);
        tasks.put("configured", configured);
        tasks.put("needPk", needPk);
        lineageTableNode.setTasks(tasks);

        Map<String, DAG> taskDagMap = new HashMap<>();
        taskDagMap.put("u3", upstreamNoTarget);
        taskDagMap.put("u4", upstreamConfigured);
        taskDagMap.put("u5", upstreamNeedPk);

        doReturn(List.of("pk1")).when(spy).getPrimaryOrUniqueKeyFieldsForNodeOrSource("u5", upstreamTarget2);

        assertEquals(List.of("cfg1"), spy.tryGetUpdateFieldsFromUpstreamTarget(lineageTableNode, "current", taskDagMap));

        lineageTableNode.setTasks(Map.of("needPk", needPk));
        assertEquals(List.of("pk1"), spy.tryGetUpdateFieldsFromUpstreamTarget(lineageTableNode, "current", taskDagMap));
    }

    @Test
    void tableProperties_shouldCreateOrReuseExistingTableProperties() {
        LineageTableNode lineageTableNode = new LineageTableNode("t", "c", "cn", "pdk", null);
        lineageTableNode.setId("n1");
        Map<String, Object> attrs = new HashMap<>();

        service.tableProperties("task", attrs, lineageTableNode, List.of("f1"));
        Object created = attrs.get("task");
        assertTrue(created instanceof TableProperties);
        assertEquals(List.of("f1"), ((TableProperties) created).getUpdateConditionField());

        TableProperties existing = new TableProperties();
        attrs.put("task", existing);
        service.tableProperties("task", attrs, lineageTableNode, List.of("f2"));
        assertSame(existing, attrs.get("task"));
        assertEquals(List.of("f2"), existing.getUpdateConditionField());
    }

    @Test
    void eachDagNode_shouldSkipInvalidNodesAndPopulateUpdateConditionFields() {
        FieldOriginalNameMapping spy = spy(service);

        LineageTableNode blankId = new LineageTableNode("t", "c", "cn", "pdk", null);
        blankId.setId(" ");
        blankId.setTasks(Map.of("t1", new LineageTask("t1", "n", null, null, null, null)));

        LineageTableNode emptyTasks = new LineageTableNode("t", "c", "cn", "pdk", null);
        emptyTasks.setId("emptyTasks");
        emptyTasks.setTasks(new HashMap<>());

        LineageTableNode node = new LineageTableNode("tbl", "conn", "cn", "pdk", null);
        node.setId("lineage");
        LineageTaskNode taskNodeTarget = new LineageTaskNode("tn1", "n", "t");
        taskNodeTarget.setTaskNodePos(LineageTaskNode.TASK_NODE_TARGET_POS);
        LineageTask taskConfigured = new LineageTask("507f1f77bcf86cd799439011", "n", taskNodeTarget, null, null, null);

        LineageTaskNode taskNodeTarget2 = new LineageTaskNode("tn2", "n", "t");
        taskNodeTarget2.setTaskNodePos(LineageTaskNode.TASK_NODE_TARGET_POS);
        LineageTask taskNeedPk = new LineageTask("invalid", "n", taskNodeTarget2, null, null, null);

        LineageTaskNode taskNodeSource = new LineageTaskNode(null, "n", "t");
        taskNodeSource.setTaskNodePos(LineageTaskNode.TASK_NODE_SOURCE_POS);
        LineageTask taskFallbackSource = new LineageTask("taskSource", "n", taskNodeSource, null, null, null);

        LineageTaskNode taskNodeTargetNoTable = new LineageTaskNode("notExists", "n", "t");
        taskNodeTargetNoTable.setTaskNodePos(LineageTaskNode.TASK_NODE_TARGET_POS);
        LineageTask taskNoUpdateFields = new LineageTask("taskEmpty", "n", taskNodeTargetNoTable, null, null, null);

        Map<String, LineageTask> tasks = new HashMap<>();
        tasks.put("null", null);
        tasks.put("blank", new LineageTask(" ", "n", taskNodeTarget, null, null, null));
        tasks.put("cfg", taskConfigured);
        tasks.put("pk", taskNeedPk);
        tasks.put("src", taskFallbackSource);
        tasks.put("empty", taskNoUpdateFields);
        node.setTasks(tasks);

        Graph<Node, Edge> g1 = new Graph<>();
        TableNode tn1 = new TableNode();
        tn1.setId("tn1");
        tn1.setConnectionId("conn");
        tn1.setTableName("tbl");
        tn1.setUpdateConditionFields(List.of("u1"));
        g1.setNode("tn1", tn1);
        DAG dag1 = new DAG(g1);

        Graph<Node, Edge> g2 = new Graph<>();
        TableNode tn2 = new TableNode();
        tn2.setId("tn2");
        tn2.setConnectionId("conn");
        tn2.setTableName("tbl");
        tn2.setUpdateConditionFields(new ArrayList<>());
        g2.setNode("tn2", tn2);
        DAG dag2 = new DAG(g2);

        DAG dag3 = new DAG(new Graph<>());
        DAG dag4 = new DAG(new Graph<>());

        Map<String, DAG> taskDagMap = new HashMap<>();
        taskDagMap.put("507f1f77bcf86cd799439011", dag1);
        taskDagMap.put("invalid", dag2);
        taskDagMap.put("taskSource", dag3);
        taskDagMap.put("taskEmpty", dag4);

        doReturn(List.of("pk1")).when(spy).getPrimaryOrUniqueKeyFieldsForNodeOrSource(any(), any(TableNode.class));
        doReturn(new ArrayList<>()).when(spy).tryGetUpdateFieldsFromUpstreamTarget(any(), any(), any());
        doReturn(List.of("srcPk")).when(spy).getPrimaryOrUniqueKeyFieldsFromSource("conn", "tbl");

        Dag lineageDag = new Dag();
        lineageDag.setNodes(List.of(new LineageTaskNode("x", "n", "t"), blankId, emptyTasks, node));
        spy.eachDagNode(lineageDag, taskDagMap);

        assertNotNull(node.getAttrs());
        assertTrue(node.getAttrs().get("507f1f77bcf86cd799439011") instanceof TableProperties);
        assertEquals(List.of("u1"), ((TableProperties) node.getAttrs().get("507f1f77bcf86cd799439011")).getUpdateConditionField());
        assertEquals(List.of("pk1"), ((TableProperties) node.getAttrs().get("invalid")).getUpdateConditionField());
        assertEquals(List.of("srcPk"), ((TableProperties) node.getAttrs().get("taskSource")).getUpdateConditionField());
        assertNull(node.getAttrs().get("taskEmpty"));
    }

    @Test
    void eachLineageTask_shouldFindTableNodeByScanAndUseConfiguredOrComputedFields() {
        FieldOriginalNameMapping spy = spy(service);
        LineageTableNode lineageTableNode = new LineageTableNode("tbl", "conn", "cn", "pdk", null);
        lineageTableNode.setId("lineage");
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("task", new TableProperties());

        Graph<Node, Edge> g = new Graph<>();
        TableNode tn = new TableNode();
        tn.setId("n1");
        tn.setConnectionId("conn");
        tn.setTableName("tbl");
        tn.setUpdateConditionFields(new ArrayList<>());
        g.setNode("n1", tn);
        DAG taskDag = new DAG(g);

        LineageTaskNode taskNode = new LineageTaskNode("", "n", "t");
        taskNode.setTaskNodePos(LineageTaskNode.TASK_NODE_TARGET_POS);
        LineageTask lineageTask = new LineageTask("task", "n", taskNode, null, null, null);

        doReturn(List.of("pkX")).when(spy).getPrimaryOrUniqueKeyFieldsForNodeOrSource(any(), any(TableNode.class));
        spy.eachLineageTask(lineageTask, lineageTableNode, attrs, taskDag, new HashMap<>());

        assertSame(attrs.get("task"), attrs.get("task"));
        assertEquals(List.of("pkX"), ((TableProperties) attrs.get("task")).getUpdateConditionField());
    }

    @Test
    void findUpdateConditionField_shouldReturnEarlyOrDelegateToEachDagNode() {
        FieldOriginalNameMapping spy = spy(service);
        spy.findUpdateConditionField(null, new HashMap<>(), new HashMap<>());

        Dag empty = new Dag();
        empty.setNodes(new ArrayList<>());
        spy.findUpdateConditionField(empty, new HashMap<>(), new HashMap<>());

        Dag lineageDag = new Dag();
        LineageTableNode finalTarget = new LineageTableNode("t", "c", "cn", "pdk", null);
        finalTarget.setId("final");
        lineageDag.setNodes(List.of(finalTarget));
        lineageDag.setEdges(new ArrayList<>());

        Map<String, Map<String, String>> mapping = new HashMap<>();
        mapping.put("final", Map.of("a", "oa", " ", "x", "b", ""));

        spy.findUpdateConditionField(lineageDag, Map.of("task", new DAG(new Graph<>())), mapping);
        verify(spy, times(1)).eachDagNode(any(), any());
    }

    private static Field field(String fieldName, String originalFieldName, boolean deleted, boolean pk, Integer pkPos) {
        Field f = new Field();
        f.setFieldName(fieldName);
        f.setOriginalFieldName(originalFieldName);
        f.setDeleted(deleted);
        f.setPrimaryKey(pk);
        f.setPrimaryKeyPosition(pkPos);
        return f;
    }
}
