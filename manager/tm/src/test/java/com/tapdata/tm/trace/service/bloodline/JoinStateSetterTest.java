package com.tapdata.tm.trace.service.bloodline;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.lineage.analyzer.entity.LineageMetadataInstance;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTask;
import com.tapdata.tm.trace.dto.boodline.FieldNameMapping;
import com.tapdata.tm.trace.dto.boodline.NodeFieldState;
import com.tapdata.tm.trace.dto.boodline.TableProperties;
import com.tapdata.tm.trace.dto.boodline.TracedField;
import io.github.openlg.graphlib.Graph;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JoinStateSetterTest {
    private JoinStateSetter service;
    private FieldOriginalNameMapping fieldOriginalNameMapping;

    @BeforeEach
    void setUp() {
        service = new JoinStateSetter();
        fieldOriginalNameMapping = spy(new FieldOriginalNameMapping());
        service.fieldOriginalNameMapping = fieldOriginalNameMapping;
    }

    @Test
    void nodeType_shouldCoverAllBranches() {
        assertEquals("JOIN", service.nodeType(true, true, true));
        assertEquals("MERGE", service.nodeType(false, true, true));
        assertEquals("APPEND", service.nodeType(false, false, true));
        assertEquals("OTHER", service.nodeType(false, false, false));
    }

    @Test
    void markJoinState_shouldReturnEarly_whenDagNullOrEmpty() {
        service.markJoinState(null, new HashMap<>(), new HashMap<>());
        Dag empty = new Dag();
        empty.setNodes(new ArrayList<>());
        service.markJoinState(empty, new HashMap<>(), new HashMap<>());
    }

    @Test
    void pickBestTargetFieldName_shouldCoverAllBranches() {
        assertNull(service.pickBestTargetFieldName(null, "a"));
        assertNull(service.pickBestTargetFieldName(List.of("a"), " "));
        assertEquals("a", service.pickBestTargetFieldName(List.of("a"), "a"));
        assertEquals("x", service.pickBestTargetFieldName(List.of("x", "y"), "x"));
        assertEquals("a", service.pickBestTargetFieldName(List.of("b", "a"), "x"));
        assertNull(service.pickBestTargetFieldName(List.of(" ", " "), "x"));
    }

    @Test
    void flattenAndContainsAppendMergeType_shouldCoverAllBranches() {
        MergeTableProperties child = new MergeTableProperties();
        child.setId("c");
        child.setMergeType(MergeTableProperties.MergeType.appendWrite);

        MergeTableProperties root = new MergeTableProperties();
        root.setId("r");
        root.setMergeType(MergeTableProperties.MergeType.updateWrite);
        root.setChildren(List.of(child));

        MergeTableNode mergeTableNode = new MergeTableNode();
        mergeTableNode.setMergeProperties(List.of(root));

        assertTrue(service.containsAppendMergeType(mergeTableNode));
        assertEquals(2, service.flattenMergeProperties(List.of(root)).size());
        assertFalse(service.containsAppendMergeType(new MergeTableNode()));
        assertEquals(0, service.flattenMergeProperties(null).size());
    }

    @Test
    void toTableKey_shouldHandleBlankInputs() {
        assertNull(service.toTableKey(null, "t"));
        assertNull(service.toTableKey("c", " "));
        assertEquals("c_t", service.toTableKey("c", "t"));
    }

    @Test
    void resolveNodeTableNameAndResolvePredecessorTableName_shouldCoverBranches() {
        Graph<Node, Edge> g = new Graph<>();
        DAG dag = new DAG(g);
        assertNull(service.resolveNodeTableName(dag, null));
        assertNull(service.resolveNodeTableName(null, "n1"));

        TableNode t1 = new TableNode();
        t1.setId("t1");
        t1.setTableName("table1");
        g.setNode("t1", t1);
        assertEquals("table1", service.resolveNodeTableName(dag, "t1"));
        assertEquals("fallback", service.resolvePredecessorTableName(dag, "missing", "fallback"));
        assertEquals("table1", service.resolvePredecessorTableName(dag, "t1", "fallback"));
    }

    @Test
    void isSameFieldLineage_shouldCoverAllBranches() {
        assertFalse(service.isSameFieldLineage(null, null));

        Field currentNoIds = new Field();
        Field pre = new Field();
        assertFalse(service.isSameFieldLineage(currentNoIds, pre));

        Field current = new Field();
        current.setId("id1");
        Field preWithId = new Field();
        preWithId.setId("id1");
        assertTrue(service.isSameFieldLineage(current, preWithId));

        Field preWithOld = new Field();
        preWithOld.setOldIdList(List.of("id1"));
        assertTrue(service.isSameFieldLineage(current, preWithOld));

        Field preNoMatch = new Field();
        preNoMatch.setId("id2");
        preNoMatch.setOldIdList(List.of("id3", " "));
        assertFalse(service.isSameFieldLineage(current, preNoMatch));
    }

    @Test
    void findRootNodeIds_shouldFindRootsOrReturnEmpty() {
        Graph<Node, Edge> g = new Graph<>();
        DAG dag = new DAG(g);
        assertEquals(Set.of(), service.findRootNodeIds(dag, "missing"));
        assertEquals(Set.of(), service.findRootNodeIds(null, "n1"));
        assertEquals(Set.of(), service.findRootNodeIds(dag, " "));

        TableNode r = new TableNode();
        r.setId("r");
        TableNode p = new TableNode();
        p.setId("p");
        g.setNode("r", r);
        g.setNode("p", p);
        g.setEdge("r", "p", new Edge("r", "p"));

        assertEquals(Set.of("r"), service.findRootNodeIds(dag, "p"));
        assertEquals(Set.of("r"), service.findRootNodeIds(dag, "r"));
    }

    @Test
    void traceToRoot_shouldReturnEmptyOrRootAndCoverPredecessorFilteringBugPath() {
        Graph<Node, Edge> g = new Graph<>();
        DAG dag = new DAG(g);
        dag.setTaskId(new ObjectId("507f1f77bcf86cd799439011"));

        TableNode n1 = new TableNode();
        n1.setId("n1");
        n1.setTableName("t1");
        g.setNode("n1", n1);

        doReturn(field("f1", "id1", null, null)).when(fieldOriginalNameMapping)
                .getField(eq("task"), eq("n1"), any(), eq("f1"), anyMap());

        Optional<TracedField> ok = service.traceToRoot(dag, "task", "n1", "t1", "f1", new HashMap<>());
        assertTrue(ok.isPresent());
        assertEquals("n1", ok.get().getRootNodeId());
        assertEquals("f1", ok.get().getRootFieldName());

        TableNode n2 = new TableNode();
        n2.setId("n2");
        n2.setTableName("t2");
        g.setNode("n2", n2);
        g.setEdge("n1", "n2", new Edge("n1", "n2"));

        doReturn(field("f1", "id1", null, null)).when(fieldOriginalNameMapping)
                .getField(eq("task"), eq("n2"), any(), eq("f1"), anyMap());

        Optional<TracedField> empty = service.traceToRoot(dag, "task", "n2", "t2", "f1", new HashMap<>());
        assertTrue(empty.isEmpty());

        assertTrue(service.traceToRoot(null, "task", "n1", "t1", "f1", new HashMap<>()).isEmpty());
        assertTrue(service.traceToRoot(dag, "", "n1", "t1", "f1", new HashMap<>()).isEmpty());
        assertTrue(service.traceToRoot(dag, "task", "", "t1", "f1", new HashMap<>()).isEmpty());
        assertTrue(service.traceToRoot(dag, "task", "n1", "t1", "", new HashMap<>()).isEmpty());

        doReturn(null).when(fieldOriginalNameMapping)
                .getField(eq("task"), eq("n1"), any(), eq("missing"), anyMap());
        assertTrue(service.traceToRoot(dag, "task", "n1", "t1", "missing", new HashMap<>()).isEmpty());
    }

    @Test
    void traceToRoot_shouldDetectCycle_andCoverPreviousFieldNameBranch() {
        JoinStateSetter spySetter = spy(service);
        spySetter.fieldOriginalNameMapping = fieldOriginalNameMapping;

        Graph<Node, Edge> g = new Graph<>();
        DAG dag = new DAG(g);
        dag.setTaskId(new ObjectId("507f1f77bcf86cd799439011"));

        TableNode blank = new TableNode();
        blank.setId(" ");
        blank.setTableName("tb");
        TableNode n2 = new TableNode();
        n2.setId("n2");
        n2.setTableName("t2");
        g.setNode(" ", blank);
        g.setNode("n2", n2);
        g.setEdge(" ", "n2", new Edge(" ", "n2"));

        doReturn(field("f1", "id1", "pf", null)).when(fieldOriginalNameMapping)
                .getField(eq("task"), eq("n2"), any(), eq("f1"), anyMap());
        doReturn(field("pf", "id1", null, null)).when(fieldOriginalNameMapping)
                .getField(eq("task"), eq(" "), any(), eq("pf"), anyMap());

        doReturn(new NodeFieldState("n2", "t2", "f1"))
                .when(spySetter).matchedNodeFieldState(eq("task"), any(), anyList(), eq(dag), anyMap(), any(Field.class));

        assertTrue(spySetter.traceToRoot(dag, "task", "n2", "t2", "f1", new HashMap<>()).isEmpty());
    }

    @Test
    void matchedNodeFieldState_shouldReturnMatchedOrNullOnAmbiguous() {
        Graph<Node, Edge> g = new Graph<>();
        DAG dag = new DAG(g);
        TableNode p1 = new TableNode();
        p1.setId("p1");
        p1.setTableName("t");
        TableNode p2 = new TableNode();
        p2.setId("p2");
        p2.setTableName("t");
        g.setNode("p1", p1);
        g.setNode("p2", p2);

        Field current = new Field();
        current.setId("id");
        Field pre1 = new Field();
        pre1.setId("id");
        pre1.setFieldName("f1");
        Field pre2 = new Field();
        pre2.setId("id");
        pre2.setFieldName("f2");

        doReturn(Map.of("f1", pre1)).when(fieldOriginalNameMapping).getFieldMapForNode(eq("task"), eq("p1"), any(), anyMap());
        doReturn(new HashMap<>()).when(fieldOriginalNameMapping).getFieldMapForNode(eq("task"), eq("p2"), any(), anyMap());
        NodeFieldState matched = service.matchedNodeFieldState("task", "t", List.of(p1, p2), dag, new HashMap<>(), current);
        assertNotNull(matched);
        assertEquals("p1", matched.getNodeId());

        doReturn(Map.of("f2", pre2)).when(fieldOriginalNameMapping).getFieldMapForNode(eq("task"), eq("p2"), any(), anyMap());
        NodeFieldState ambiguous = service.matchedNodeFieldState("task", "t", List.of(p1, p2), dag, new HashMap<>(), current);
        assertNull(ambiguous);
    }

    @Test
    void mergeInfoSetter_andCollectChildrenJoinKeys_shouldCoverEarlyReturns() {
        LineageTableNode notTable = lineageNode("c", "t", "id");
        notTable.setMetadata(null);
        service.mergeInfoSetter(notTable, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>());

        LineageTableNode emptyTasks = lineageNode("c", "t", "id2");
        emptyTasks.setTasks(new HashMap<>());
        service.mergeInfoSetter(emptyTasks, new HashMap<>(), new HashMap<>(), new HashMap<>(), Map.of("t", new HashMap<>()));

        service.collectChildrenJoinKeys(null, new HashMap<>(), new DAG(new Graph<>()));
        service.collectChildrenJoinKeys(new ArrayList<>(), new HashMap<>(), new DAG(new Graph<>()));
    }

    @Test
    void newTableProperties_shouldSetRootAndPreNode() {
        TableProperties p = JoinStateSetter.newTableProperties("r", "p");
        assertEquals("r", p.getRootNodeId());
        assertEquals("p", p.getPreNodeId());
    }

    private static JoinProcessorNode.JoinExpression expr(String left, String right) {
        JoinProcessorNode.JoinExpression e = new JoinProcessorNode.JoinExpression();
        e.setLeft(left);
        e.setRight(right);
        return e;
    }

    private static LineageTableNode lineageNode(String connectionId, String table, String id) {
        LineageMetadataInstance metadata = new LineageMetadataInstance();
        metadata.setSourceType("SOURCE");
        LineageTableNode n = new LineageTableNode(table, connectionId, "cn", "pdk", metadata);
        n.setId(id);
        return n;
    }

    private static Field field(String fieldName, String id, String previousFieldName, List<String> oldIds) {
        Field f = new Field();
        f.setFieldName(fieldName);
        f.setId(id);
        f.setPreviousFieldName(previousFieldName);
        f.setOldIdList(oldIds);
        return f;
    }
}
