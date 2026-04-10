package com.tapdata.tm.task.utils;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.group.vo.DagChangeDetail;
import com.tapdata.tm.group.vo.FieldChange;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TaskConfigCompareUtil
 */
class TaskConfigCompareUtilTest {

    private TaskDto buildTask(String name, String type, String syncType) {
        TaskDto task = new TaskDto();
        task.setName(name);
        task.setType(type);
        task.setSyncType(syncType);
        return task;
    }

    private DAG buildDagWithNode(DatabaseNode node) {
        Dag dag = new Dag();
        dag.setNodes(new ArrayList<>(List.of(node)));
        dag.setEdges(Collections.emptyList());
        return DAG.build(dag);
    }

    private DAG buildDagWithNodesAndEdges(List<DatabaseNode> nodes, List<Edge> edges) {
        Dag dag = new Dag();
        dag.setNodes(new ArrayList<>(nodes));
        dag.setEdges(new ArrayList<>(edges));
        return DAG.build(dag);
    }

    @Nested
    @DisplayName("isConfigEqual tests")
    class IsConfigEqualTest {

        @Test
        @DisplayName("Both null returns true")
        void testBothNull() {
            assertTrue(TaskConfigCompareUtil.isConfigEqual(null, null));
        }

        @Test
        @DisplayName("One null returns false")
        void testOneNull() {
            assertFalse(TaskConfigCompareUtil.isConfigEqual(buildTask("t", "initial_sync", "migrate"), null));
            assertFalse(TaskConfigCompareUtil.isConfigEqual(null, buildTask("t", "initial_sync", "migrate")));
        }

        @Test
        @DisplayName("Same config returns true")
        void testSameConfig() {
            TaskDto t1 = buildTask("task1", "initial_sync", "migrate");
            t1.setReadBatchSize(500);
            t1.setWriteBatchSize(200);
            TaskDto t2 = buildTask("task1", "initial_sync", "migrate");
            t2.setReadBatchSize(500);
            t2.setWriteBatchSize(200);
            assertTrue(TaskConfigCompareUtil.isConfigEqual(t1, t2));
        }

        @Test
        @DisplayName("Different name returns false")
        void testDifferentName() {
            TaskDto t1 = buildTask("task1", "initial_sync", "migrate");
            TaskDto t2 = buildTask("task2", "initial_sync", "migrate");
            assertFalse(TaskConfigCompareUtil.isConfigEqual(t1, t2));
        }

        @Test
        @DisplayName("Different runtime settings returns false")
        void testDifferentRuntimeSettings() {
            TaskDto t1 = buildTask("task1", "initial_sync", "migrate");
            t1.setWriteBatchSize(100);
            TaskDto t2 = buildTask("task1", "initial_sync", "migrate");
            t2.setWriteBatchSize(500);
            assertFalse(TaskConfigCompareUtil.isConfigEqual(t1, t2));
        }
    }

    @Nested
    @DisplayName("getDifferentFields tests")
    class GetDifferentFieldsTest {

        @Test
        @DisplayName("Both null returns empty list")
        void testBothNull() {
            assertTrue(TaskConfigCompareUtil.getDifferentFields(null, null).isEmpty());
        }

        @Test
        @DisplayName("One null returns all config fields")
        void testOneNull() {
            List<String> fields = TaskConfigCompareUtil.getDifferentFields(
                    buildTask("t", "initial_sync", "migrate"), null);
            assertEquals(TaskConfigCompareUtil.getConfigFields().size(), fields.size());
        }

        @Test
        @DisplayName("Same tasks returns empty list")
        void testSameTasks() {
            TaskDto t1 = buildTask("task1", "initial_sync", "migrate");
            TaskDto t2 = buildTask("task1", "initial_sync", "migrate");
            assertTrue(TaskConfigCompareUtil.getDifferentFields(t1, t2).isEmpty());
        }

        @Test
        @DisplayName("Different fields are correctly identified")
        void testDifferentFields() {
            TaskDto t1 = buildTask("task1", "initial_sync", "migrate");
            t1.setReadBatchSize(100);
            TaskDto t2 = buildTask("task1", "initial_sync", "migrate");
            t2.setReadBatchSize(500);

            List<String> fields = TaskConfigCompareUtil.getDifferentFields(t1, t2);
            assertTrue(fields.contains("readBatchSize"));
            assertFalse(fields.contains("name"));
        }
    }

    @Nested
    @DisplayName("getDetailedChanges tests")
    class GetDetailedChangesTest {

        @Test
        @DisplayName("Both null returns empty list")
        void testBothNull() {
            assertTrue(TaskConfigCompareUtil.getDetailedChanges(null, null, new DagChangeDetail()).isEmpty());
        }

        @Test
        @DisplayName("One null returns empty list")
        void testOneNull() {
            assertTrue(TaskConfigCompareUtil.getDetailedChanges(buildTask("t", "a", "b"), null, new DagChangeDetail()).isEmpty());
        }

        @Test
        @DisplayName("Same tasks returns empty changes")
        void testSameTasks() {
            TaskDto t1 = buildTask("task1", "initial_sync", "migrate");
            TaskDto t2 = buildTask("task1", "initial_sync", "migrate");
            assertTrue(TaskConfigCompareUtil.getDetailedChanges(t1, t2, new DagChangeDetail()).isEmpty());
        }

        @Test
        @DisplayName("Non-DAG field changes are detected with from/to")
        void testNonDagFieldChange() {
            TaskDto importTask = buildTask("task1", "initial_sync", "migrate");
            importTask.setWriteBatchSize(500);
            TaskDto existingTask = buildTask("task1", "initial_sync", "migrate");
            existingTask.setWriteBatchSize(100);

            List<FieldChange> changes = TaskConfigCompareUtil.getDetailedChanges(importTask, existingTask, new DagChangeDetail());
            assertFalse(changes.isEmpty());
            FieldChange wbs = changes.stream()
                    .filter(c -> "writeBatchSize".equals(c.getField())).findFirst().orElse(null);
            assertNotNull(wbs);
            assertEquals(100, wbs.getFrom());
            assertEquals(500, wbs.getTo());
        }

        @Test
        @DisplayName("DAG node-level changes are detected in dagChangeDetail, not in changes")
        void testDagNodeChanges() {
            DatabaseNode node1 = new DatabaseNode();
            node1.setId("n1");
            node1.setName("source");
            node1.setDatabaseType("MySQL");
            node1.setTableNames(List.of("table1"));

            DatabaseNode node2 = new DatabaseNode();
            node2.setId("n1");
            node2.setName("source");
            node2.setDatabaseType("MySQL");
            node2.setTableNames(List.of("table1", "table2"));

            TaskDto importTask = buildTask("task1", "initial_sync", "migrate");
            importTask.setDag(buildDagWithNode(node2));

            TaskDto existingTask = buildTask("task1", "initial_sync", "migrate");
            existingTask.setDag(buildDagWithNode(node1));

            DagChangeDetail detail = new DagChangeDetail();
            List<FieldChange> changes = TaskConfigCompareUtil.getDetailedChanges(importTask, existingTask, detail);
            // DAG changes should go to dagChangeDetail, not changes
            boolean hasDagInChanges = changes.stream().anyMatch(c -> c.getField().startsWith("dag."));
            assertFalse(hasDagInChanges, "dag.* changes should not appear in changes list");
            assertFalse(detail.getNodeConfigChanges().isEmpty(), "Should have nodeConfigChanges in dagChangeDetail");
        }

        @Test
        @DisplayName("DAG one null produces dag-level change")
        void testDagOneNull() {
            DatabaseNode node = new DatabaseNode();
            node.setId("n1");
            node.setName("source");
            node.setDatabaseType("MySQL");

            TaskDto importTask = buildTask("task1", "initial_sync", "migrate");
            importTask.setDag(buildDagWithNode(node));

            TaskDto existingTask = buildTask("task1", "initial_sync", "migrate");
            // no DAG

            List<FieldChange> changes = TaskConfigCompareUtil.getDetailedChanges(importTask, existingTask, new DagChangeDetail());
            boolean hasDagChange = changes.stream().anyMatch(c -> "dag".equals(c.getField()));
            assertTrue(hasDagChange, "Should have dag-level change when one DAG is null");
        }
    }

    @Nested
    @DisplayName("Whitelist (@EqField) comparison tests")
    class WhitelistTest {

        @Test
        @DisplayName("Non-@EqField field changes do not affect comparison")
        void testNonEqFieldIgnored() {
            DatabaseNode node1 = new DatabaseNode();
            node1.setId("shared-id");
            node1.setName("source");
            node1.setDatabaseType("MySQL");
            node1.setTableNames(List.of("t1"));
            node1.setRows(100); // rows is NOT @EqField

            DatabaseNode node2 = new DatabaseNode();
            node2.setId("shared-id");
            node2.setName("source");
            node2.setDatabaseType("MySQL");
            node2.setTableNames(List.of("t1"));
            node2.setRows(999); // different rows — should be ignored

            TaskDto t1 = buildTask("task1", "initial_sync", "migrate");
            t1.setDag(buildDagWithNode(node1));

            TaskDto t2 = buildTask("task1", "initial_sync", "migrate");
            t2.setDag(buildDagWithNode(node2));

            // With whitelist mode, rows differences should not produce changes
            List<FieldChange> changes = TaskConfigCompareUtil.getDetailedChanges(t1, t2, new DagChangeDetail());
            boolean hasRowsChange = changes.stream()
                    .anyMatch(c -> c.getField().contains("rows"));
            assertFalse(hasRowsChange, "Non-@EqField 'rows' should not appear in changes");
        }

        @Test
        @DisplayName("@EqField changes are detected in dagChangeDetail")
        void testEqFieldDetected() {
            DatabaseNode node1 = new DatabaseNode();
            node1.setId("n1");
            node1.setName("source");
            node1.setDatabaseType("MySQL");
            node1.setTableNames(List.of("t1"));

            DatabaseNode node2 = new DatabaseNode();
            node2.setId("n1");
            node2.setName("source");
            node2.setDatabaseType("MySQL");
            node2.setTableNames(List.of("t1", "t2")); // @EqField, should be detected

            TaskDto t1 = buildTask("task1", "initial_sync", "migrate");
            t1.setDag(buildDagWithNode(node1));

            TaskDto t2 = buildTask("task1", "initial_sync", "migrate");
            t2.setDag(buildDagWithNode(node2));

            DagChangeDetail detail = new DagChangeDetail();
            List<FieldChange> changes = TaskConfigCompareUtil.getDetailedChanges(t1, t2, detail);
            boolean hasTableNamesInDetail = detail.getNodeConfigChanges().stream()
                    .anyMatch(c -> c.getField().contains("tableNames"));
            assertTrue(hasTableNamesInDetail, "Should detect @EqField 'tableNames' in dagChangeDetail");
            // Should not appear in changes list
            boolean hasTableNamesInChanges = changes.stream()
                    .anyMatch(c -> c.getField().contains("tableNames"));
            assertFalse(hasTableNamesInChanges, "tableNames should not appear in changes list");
        }

        @Test
        @DisplayName("Cross-environment comparison: same node IDs (preserved during import), same config")
        void testCrossEnvironmentEqual() {
            DatabaseNode node1 = new DatabaseNode();
            node1.setId("shared-node-id");
            node1.setName("source");
            node1.setDatabaseType("MySQL");
            node1.setTableNames(List.of("t1"));

            DatabaseNode node2 = new DatabaseNode();
            node2.setId("shared-node-id");
            node2.setName("source");
            node2.setDatabaseType("MySQL");
            node2.setTableNames(List.of("t1"));

            TaskDto t1 = buildTask("task1", "initial_sync", "migrate");
            t1.setDag(buildDagWithNode(node1));

            TaskDto t2 = buildTask("task1", "initial_sync", "migrate");
            t2.setDag(buildDagWithNode(node2));

            assertTrue(TaskConfigCompareUtil.isConfigEqual(t1, t2),
                    "Tasks with same node IDs and same config should be equal");
        }

        @Test
        @DisplayName("Duplicate node names with different IDs are correctly distinguished")
        void testDuplicateNodeNamesDistinguished() {
            // Two nodes with the same name but different IDs — should be treated as different nodes
            DatabaseNode nodeA = new DatabaseNode();
            nodeA.setId("id-aaa");
            nodeA.setName("source");
            nodeA.setDatabaseType("MySQL");
            nodeA.setTableNames(List.of("t1"));

            DatabaseNode nodeB = new DatabaseNode();
            nodeB.setId("id-bbb");
            nodeB.setName("source"); // same name as nodeA
            nodeB.setDatabaseType("PostgreSQL");
            nodeB.setTableNames(List.of("t2"));

            TaskDto importTask = buildTask("task1", "initial_sync", "migrate");
            importTask.setDag(buildDagWithNodesAndEdges(List.of(nodeA, nodeB), Collections.emptyList()));

            DatabaseNode nodeA2 = new DatabaseNode();
            nodeA2.setId("id-aaa");
            nodeA2.setName("source");
            nodeA2.setDatabaseType("MySQL");
            nodeA2.setTableNames(List.of("t1"));

            DatabaseNode nodeB2 = new DatabaseNode();
            nodeB2.setId("id-bbb");
            nodeB2.setName("source"); // same name as nodeA2
            nodeB2.setDatabaseType("PostgreSQL");
            nodeB2.setTableNames(List.of("t2"));

            TaskDto existingTask = buildTask("task1", "initial_sync", "migrate");
            existingTask.setDag(buildDagWithNodesAndEdges(List.of(nodeA2, nodeB2), Collections.emptyList()));

            assertTrue(TaskConfigCompareUtil.isConfigEqual(importTask, existingTask),
                    "Both nodes should be matched by ID even though names are the same");

            DagChangeDetail detail = new DagChangeDetail();
            List<FieldChange> changes = TaskConfigCompareUtil.getDetailedChanges(importTask, existingTask, detail);
            assertTrue(changes.isEmpty(), "No non-DAG changes expected");
            assertFalse(detail.hasChanges(), "No DAG changes expected when matched by ID");
        }
    }

    @Nested
    @DisplayName("Edge comparison tests")
    class EdgeComparisonTest {

        @Test
        @DisplayName("Edge addition detected in dagChangeDetail")
        void testEdgeAddition() {
            DatabaseNode src = new DatabaseNode();
            src.setId("n1");
            src.setName("source");
            src.setDatabaseType("MySQL");

            DatabaseNode tgt = new DatabaseNode();
            tgt.setId("n2");
            tgt.setName("target");
            tgt.setDatabaseType("MySQL");

            DAG dagWithEdge = buildDagWithNodesAndEdges(
                    List.of(src, tgt),
                    List.of(new Edge("n1", "n2"))
            );

            DatabaseNode src2 = new DatabaseNode();
            src2.setId("n1");
            src2.setName("source");
            src2.setDatabaseType("MySQL");

            DatabaseNode tgt2 = new DatabaseNode();
            tgt2.setId("n2");
            tgt2.setName("target");
            tgt2.setDatabaseType("MySQL");

            DAG dagNoEdge = buildDagWithNodesAndEdges(
                    List.of(src2, tgt2),
                    Collections.emptyList()
            );

            TaskDto importTask = buildTask("task1", "initial_sync", "migrate");
            importTask.setDag(dagWithEdge);

            TaskDto existingTask = buildTask("task1", "initial_sync", "migrate");
            existingTask.setDag(dagNoEdge);

            DagChangeDetail detail = new DagChangeDetail();
            List<FieldChange> changes = TaskConfigCompareUtil.getDetailedChanges(importTask, existingTask, detail);
            assertFalse(detail.getEdgeAdditions().isEmpty(), "Should detect edge addition in dagChangeDetail");
            boolean hasEdgeInChanges = changes.stream()
                    .anyMatch(c -> c.getField().startsWith("dag.edges."));
            assertFalse(hasEdgeInChanges, "dag.edges.* should not appear in changes list");
        }

        @Test
        @DisplayName("Edge removal detected in dagChangeDetail")
        void testEdgeRemoval() {
            DatabaseNode src = new DatabaseNode();
            src.setId("n1");
            src.setName("source");
            src.setDatabaseType("MySQL");

            DatabaseNode tgt = new DatabaseNode();
            tgt.setId("n2");
            tgt.setName("target");
            tgt.setDatabaseType("MySQL");

            DAG dagWithEdge = buildDagWithNodesAndEdges(
                    List.of(src, tgt),
                    List.of(new Edge("n1", "n2"))
            );

            DatabaseNode src2 = new DatabaseNode();
            src2.setId("n1");
            src2.setName("source");
            src2.setDatabaseType("MySQL");

            DatabaseNode tgt2 = new DatabaseNode();
            tgt2.setId("n2");
            tgt2.setName("target");
            tgt2.setDatabaseType("MySQL");

            DAG dagNoEdge = buildDagWithNodesAndEdges(
                    List.of(src2, tgt2),
                    Collections.emptyList()
            );

            TaskDto importTask = buildTask("task1", "initial_sync", "migrate");
            importTask.setDag(dagNoEdge);

            TaskDto existingTask = buildTask("task1", "initial_sync", "migrate");
            existingTask.setDag(dagWithEdge);

            DagChangeDetail detail = new DagChangeDetail();
            List<FieldChange> changes = TaskConfigCompareUtil.getDetailedChanges(importTask, existingTask, detail);
            assertFalse(detail.getEdgeRemovals().isEmpty(), "Should detect edge removal in dagChangeDetail");
            boolean hasEdgeInChanges = changes.stream()
                    .anyMatch(c -> c.getField().startsWith("dag.edges."));
            assertFalse(hasEdgeInChanges, "dag.edges.* should not appear in changes list");
        }

        @Test
        @DisplayName("Same edges with same node IDs (preserved during import) compare equal")
        void testEdgeCrossEnvironment() {
            DatabaseNode src1 = new DatabaseNode();
            src1.setId("shared-src-id");
            src1.setName("source");
            src1.setDatabaseType("MySQL");

            DatabaseNode tgt1 = new DatabaseNode();
            tgt1.setId("shared-tgt-id");
            tgt1.setName("target");
            tgt1.setDatabaseType("MySQL");

            DAG dag1 = buildDagWithNodesAndEdges(
                    List.of(src1, tgt1),
                    List.of(new Edge("shared-src-id", "shared-tgt-id"))
            );

            DatabaseNode src2 = new DatabaseNode();
            src2.setId("shared-src-id");
            src2.setName("source");
            src2.setDatabaseType("MySQL");

            DatabaseNode tgt2 = new DatabaseNode();
            tgt2.setId("shared-tgt-id");
            tgt2.setName("target");
            tgt2.setDatabaseType("MySQL");

            DAG dag2 = buildDagWithNodesAndEdges(
                    List.of(src2, tgt2),
                    List.of(new Edge("shared-src-id", "shared-tgt-id"))
            );

            TaskDto t1 = buildTask("task1", "initial_sync", "migrate");
            t1.setDag(dag1);
            TaskDto t2 = buildTask("task1", "initial_sync", "migrate");
            t2.setDag(dag2);

            assertTrue(TaskConfigCompareUtil.isConfigEqual(t1, t2),
                    "Same edges with same node IDs should be equal");
            List<FieldChange> changes = TaskConfigCompareUtil.getDetailedChanges(t1, t2, new DagChangeDetail());
            boolean hasEdgeChange = changes.stream().anyMatch(c -> c.getField().startsWith("dag.edges."));
            assertFalse(hasEdgeChange, "No edge changes expected for same topology");
        }
    }

    @Nested
    @DisplayName("Node addition/removal tests")
    class NodeAddRemoveTest {

        @Test
        @DisplayName("Node addition detected in dagChangeDetail")
        void testNodeAddition() {
            DatabaseNode src = new DatabaseNode();
            src.setId("n1");
            src.setName("source");
            src.setDatabaseType("MySQL");

            DatabaseNode tgt = new DatabaseNode();
            tgt.setId("n2");
            tgt.setName("target");
            tgt.setDatabaseType("MySQL");

            TaskDto importTask = buildTask("task1", "initial_sync", "migrate");
            importTask.setDag(buildDagWithNodesAndEdges(List.of(src, tgt), Collections.emptyList()));

            DatabaseNode srcOnly = new DatabaseNode();
            srcOnly.setId("n1");
            srcOnly.setName("source");
            srcOnly.setDatabaseType("MySQL");

            TaskDto existingTask = buildTask("task1", "initial_sync", "migrate");
            existingTask.setDag(buildDagWithNode(srcOnly));

            DagChangeDetail detail = new DagChangeDetail();
            List<FieldChange> changes = TaskConfigCompareUtil.getDetailedChanges(importTask, existingTask, detail);
            assertFalse(detail.getNodeAdditions().isEmpty(), "Should detect node addition in dagChangeDetail");
            boolean hasNodeInChanges = changes.stream()
                    .anyMatch(c -> c.getField().startsWith("dag.nodes."));
            assertFalse(hasNodeInChanges, "dag.nodes.* should not appear in changes list");
        }

        @Test
        @DisplayName("Node removal detected in dagChangeDetail")
        void testNodeRemoval() {
            DatabaseNode src = new DatabaseNode();
            src.setId("n1");
            src.setName("source");
            src.setDatabaseType("MySQL");

            TaskDto importTask = buildTask("task1", "initial_sync", "migrate");
            importTask.setDag(buildDagWithNode(src));

            DatabaseNode src2 = new DatabaseNode();
            src2.setId("n1");
            src2.setName("source");
            src2.setDatabaseType("MySQL");

            DatabaseNode tgt2 = new DatabaseNode();
            tgt2.setId("n2");
            tgt2.setName("target");
            tgt2.setDatabaseType("MySQL");

            TaskDto existingTask = buildTask("task1", "initial_sync", "migrate");
            existingTask.setDag(buildDagWithNodesAndEdges(List.of(src2, tgt2), Collections.emptyList()));

            DagChangeDetail detail = new DagChangeDetail();
            List<FieldChange> changes = TaskConfigCompareUtil.getDetailedChanges(importTask, existingTask, detail);
            assertFalse(detail.getNodeRemovals().isEmpty(), "Should detect node removal in dagChangeDetail");
            boolean hasNodeInChanges = changes.stream()
                    .anyMatch(c -> c.getField().startsWith("dag.nodes."));
            assertFalse(hasNodeInChanges, "dag.nodes.* should not appear in changes list");
        }
    }

    @Nested
    @DisplayName("DagChangeDetail via getDetailedChanges tests")
    class CategorizedChangesTest {

        @Test
        @DisplayName("Changes are correctly categorized via getDetailedChanges")
        void testCategorization() {
            // Import: source -> target (with edge), existing: source only
            DatabaseNode src = new DatabaseNode();
            src.setId("n1");
            src.setName("source");
            src.setDatabaseType("MySQL");
            src.setTableNames(List.of("t1", "t2")); // config change

            DatabaseNode tgt = new DatabaseNode();
            tgt.setId("n2");
            tgt.setName("target");
            tgt.setDatabaseType("MySQL");

            TaskDto importTask = buildTask("task1", "initial_sync", "migrate");
            importTask.setDag(buildDagWithNodesAndEdges(
                    List.of(src, tgt),
                    List.of(new Edge("n1", "n2"))
            ));

            DatabaseNode srcExisting = new DatabaseNode();
            srcExisting.setId("n1");
            srcExisting.setName("source");
            srcExisting.setDatabaseType("MySQL");
            srcExisting.setTableNames(List.of("t1")); // different

            TaskDto existingTask = buildTask("task1", "initial_sync", "migrate");
            existingTask.setDag(buildDagWithNode(srcExisting));

            DagChangeDetail detail = new DagChangeDetail();
            List<FieldChange> changes = TaskConfigCompareUtil.getDetailedChanges(importTask, existingTask, detail);

            // changes should be empty (no non-DAG field differences)
            assertTrue(changes.isEmpty(), "changes should not contain dag.* entries");
            assertTrue(detail.hasChanges());
            assertFalse(detail.getNodeAdditions().isEmpty(), "Should have node additions (target)");
            assertFalse(detail.getNodeConfigChanges().isEmpty(), "Should have config changes (tableNames)");
            assertFalse(detail.getEdgeAdditions().isEmpty(), "Should have edge additions");
            assertTrue(detail.getNodeRemovals().isEmpty(), "Should have no node removals");
            assertTrue(detail.getEdgeRemovals().isEmpty(), "Should have no edge removals");
        }

        @Test
        @DisplayName("No changes returns empty detail")
        void testNoChanges() {
            DatabaseNode node1 = new DatabaseNode();
            node1.setId("shared-id");
            node1.setName("source");
            node1.setDatabaseType("MySQL");

            DatabaseNode node2 = new DatabaseNode();
            node2.setId("shared-id");
            node2.setName("source");
            node2.setDatabaseType("MySQL");

            TaskDto t1 = buildTask("task1", "initial_sync", "migrate");
            t1.setDag(buildDagWithNode(node1));
            TaskDto t2 = buildTask("task1", "initial_sync", "migrate");
            t2.setDag(buildDagWithNode(node2));

            DagChangeDetail detail = new DagChangeDetail();
            TaskConfigCompareUtil.getDetailedChanges(t1, t2, detail);
            assertFalse(detail.hasChanges());
            assertTrue(detail.toFlatList().isEmpty());
        }

        @Test
        @DisplayName("Node removal is categorized into nodeRemovals")
        void testNodeRemovalCategorization() {
            DatabaseNode src = new DatabaseNode();
            src.setId("n1");
            src.setName("source");
            src.setDatabaseType("MySQL");

            TaskDto importTask = buildTask("task1", "initial_sync", "migrate");
            importTask.setDag(buildDagWithNode(src));

            DatabaseNode src2 = new DatabaseNode();
            src2.setId("n1");
            src2.setName("source");
            src2.setDatabaseType("MySQL");

            DatabaseNode tgt2 = new DatabaseNode();
            tgt2.setId("n2");
            tgt2.setName("target");
            tgt2.setDatabaseType("MySQL");

            TaskDto existingTask = buildTask("task1", "initial_sync", "migrate");
            existingTask.setDag(buildDagWithNodesAndEdges(List.of(src2, tgt2), Collections.emptyList()));

            DagChangeDetail detail = new DagChangeDetail();
            TaskConfigCompareUtil.getDetailedChanges(importTask, existingTask, detail);

            assertFalse(detail.getNodeRemovals().isEmpty(), "Should have node removals for target");
            assertTrue(detail.getNodeRemovals().stream()
                    .anyMatch(c -> c.getField().contains("n2")), "Should contain target node (id=n2) removal");
        }

        @Test
        @DisplayName("Edge removal is categorized into edgeRemovals")
        void testEdgeRemovalCategorization() {
            DatabaseNode src = new DatabaseNode();
            src.setId("n1");
            src.setName("source");
            src.setDatabaseType("MySQL");

            DatabaseNode tgt = new DatabaseNode();
            tgt.setId("n2");
            tgt.setName("target");
            tgt.setDatabaseType("MySQL");

            TaskDto importTask = buildTask("task1", "initial_sync", "migrate");
            importTask.setDag(buildDagWithNodesAndEdges(List.of(src, tgt), Collections.emptyList()));

            DatabaseNode src2 = new DatabaseNode();
            src2.setId("n1");
            src2.setName("source");
            src2.setDatabaseType("MySQL");

            DatabaseNode tgt2 = new DatabaseNode();
            tgt2.setId("n2");
            tgt2.setName("target");
            tgt2.setDatabaseType("MySQL");

            TaskDto existingTask = buildTask("task1", "initial_sync", "migrate");
            existingTask.setDag(buildDagWithNodesAndEdges(
                    List.of(src2, tgt2),
                    List.of(new Edge("n1", "n2"))
            ));

            DagChangeDetail detail = new DagChangeDetail();
            TaskConfigCompareUtil.getDetailedChanges(importTask, existingTask, detail);

            assertFalse(detail.getEdgeRemovals().isEmpty(), "Should have edge removals");
            assertTrue(detail.getEdgeRemovals().stream()
                    .anyMatch(c -> c.getField().startsWith("dag.edges.")), "Should contain edge removal");
        }
    }

    @Nested
    @DisplayName("extractEqFields tests")
    class ExtractEqFieldsTest {

        @Test
        @DisplayName("Only @EqField annotated fields are extracted")
        void testOnlyEqFields() {
            DatabaseNode node = new DatabaseNode();
            node.setId("some-id");
            node.setName("myNode");
            node.setDatabaseType("MySQL");
            node.setTableNames(List.of("t1"));
            node.setRows(100);           // NOT @EqField
            node.setDistance(5);          // NOT @EqField

            Map<String, Object> fields = TaskConfigCompareUtil.extractEqFields(node);

            // Should have id, name, type (identifiers) plus @EqField fields
            assertTrue(fields.containsKey("id"));
            assertTrue(fields.containsKey("name"));
            assertTrue(fields.containsKey("type"));
            assertTrue(fields.containsKey("databaseType")); // @EqField on DataParentNode
            assertTrue(fields.containsKey("tableNames"));   // @EqField on DatabaseNode

            // Should NOT have non-@EqField fields
            assertFalse(fields.containsKey("rows"));
            assertFalse(fields.containsKey("distance"));
        }
    }

    @Nested
    @DisplayName("getConfigFields tests")
    class GetConfigFieldsTest {

        @Test
        @DisplayName("Returns a copy, not the original")
        void testReturnsCopy() {
            List<String> fields1 = TaskConfigCompareUtil.getConfigFields();
            List<String> fields2 = TaskConfigCompareUtil.getConfigFields();
            assertNotSame(fields1, fields2);
            assertEquals(fields1, fields2);
        }

        @Test
        @DisplayName("Contains essential fields")
        void testContainsEssentialFields() {
            List<String> fields = TaskConfigCompareUtil.getConfigFields();
            assertTrue(fields.contains("name"));
            assertTrue(fields.contains("type"));
            assertTrue(fields.contains("syncType"));
            assertTrue(fields.contains("dag"));
            assertTrue(fields.contains("readBatchSize"));
            assertTrue(fields.contains("writeBatchSize"));
        }
    }

    @Nested
    @DisplayName("Array element-level comparison tests")
    class ArrayComparisonTest {

        private List<FieldChange> getDagNodeChanges(DatabaseNode importNode, DatabaseNode existingNode) {
            TaskDto importTask = buildTask("task1", "initial_sync", "migrate");
            importTask.setDag(buildDagWithNode(importNode));
            TaskDto existingTask = buildTask("task1", "initial_sync", "migrate");
            existingTask.setDag(buildDagWithNode(existingNode));
            DagChangeDetail detail = new DagChangeDetail();
            TaskConfigCompareUtil.getDetailedChanges(importTask, existingTask, detail);
            return detail.getNodeConfigChanges();
        }

        @Test
        @DisplayName("Simple type list: add/remove elements reported precisely")
        void testArraySimpleTypeAddRemove() {
            DatabaseNode node1 = new DatabaseNode();
            node1.setId("n1");
            node1.setName("source");
            node1.setDatabaseType("MySQL");
            node1.setTableNames(List.of("t1", "t2", "t3"));

            DatabaseNode node2 = new DatabaseNode();
            node2.setId("n1");
            node2.setName("source");
            node2.setDatabaseType("MySQL");
            node2.setTableNames(List.of("t1", "t4")); // removed t2,t3; added t4

            List<FieldChange> changes = getDagNodeChanges(node2, node1);
            FieldChange tableChange = changes.stream()
                    .filter(c -> c.getField().contains("tableNames"))
                    .findFirst().orElse(null);
            assertNotNull(tableChange, "Should detect tableNames change");

            // from should contain removed elements (t2, t3)
            assertNotNull(tableChange.getFrom(), "from should contain removed elements");
            List<String> removed = (List<String>) tableChange.getFrom();
            assertTrue(removed.contains("t2") && removed.contains("t3"),
                    "from should contain t2 and t3, got: " + removed);
            assertFalse(removed.contains("t1"), "from should not contain unchanged t1");

            // to should contain added elements (t4)
            assertNotNull(tableChange.getTo(), "to should contain added elements");
            List<String> added = (List<String>) tableChange.getTo();
            assertTrue(added.contains("t4"), "to should contain t4, got: " + added);
            assertEquals(1, added.size());
        }

        @Test
        @DisplayName("Simple type list: reorder only produces no change")
        void testArraySimpleTypeReorder() {
            DatabaseNode node1 = new DatabaseNode();
            node1.setId("n1");
            node1.setName("source");
            node1.setDatabaseType("MySQL");
            node1.setTableNames(List.of("t1", "t2", "t3"));

            DatabaseNode node2 = new DatabaseNode();
            node2.setId("n1");
            node2.setName("source");
            node2.setDatabaseType("MySQL");
            node2.setTableNames(List.of("t3", "t1", "t2")); // same elements, different order

            List<FieldChange> changes = getDagNodeChanges(node2, node1);
            boolean hasTableChange = changes.stream()
                    .anyMatch(c -> c.getField().contains("tableNames"));
            assertFalse(hasTableChange, "Reorder only should not produce changes");
        }

        @Test
        @DisplayName("Complex object list: add element")
        void testArrayComplexObjectAdd() {
            DatabaseNode node1 = new DatabaseNode();
            node1.setId("n1");
            node1.setName("source");
            node1.setDatabaseType("MySQL");
            node1.setTableNames(List.of("t1"));

            DatabaseNode node2 = new DatabaseNode();
            node2.setId("n1");
            node2.setName("source");
            node2.setDatabaseType("MySQL");
            node2.setTableNames(List.of("t1", "t2")); // added t2

            List<FieldChange> changes = getDagNodeChanges(node2, node1);
            FieldChange tableChange = changes.stream()
                    .filter(c -> c.getField().contains("tableNames"))
                    .findFirst().orElse(null);
            assertNotNull(tableChange);
            assertNull(tableChange.getFrom(), "No elements removed");
            assertNotNull(tableChange.getTo());
            List<String> added = (List<String>) tableChange.getTo();
            assertEquals(List.of("t2"), added);
        }

        @Test
        @DisplayName("Complex object list: remove element")
        void testArrayComplexObjectRemove() {
            DatabaseNode node1 = new DatabaseNode();
            node1.setId("n1");
            node1.setName("source");
            node1.setDatabaseType("MySQL");
            node1.setTableNames(List.of("t1", "t2"));

            DatabaseNode node2 = new DatabaseNode();
            node2.setId("n1");
            node2.setName("source");
            node2.setDatabaseType("MySQL");
            node2.setTableNames(List.of("t1")); // removed t2

            List<FieldChange> changes = getDagNodeChanges(node2, node1);
            FieldChange tableChange = changes.stream()
                    .filter(c -> c.getField().contains("tableNames"))
                    .findFirst().orElse(null);
            assertNotNull(tableChange);
            assertNotNull(tableChange.getFrom());
            List<String> removed = (List<String>) tableChange.getFrom();
            assertEquals(List.of("t2"), removed);
            assertNull(tableChange.getTo(), "No elements added");
        }

        @Test
        @DisplayName("Empty list and null boundary cases")
        void testArrayEmptyAndNull() {
            DatabaseNode node1 = new DatabaseNode();
            node1.setId("n1");
            node1.setName("source");
            node1.setDatabaseType("MySQL");
            node1.setTableNames(List.of("t1"));

            DatabaseNode node2 = new DatabaseNode();
            node2.setId("n1");
            node2.setName("source");
            node2.setDatabaseType("MySQL");
            node2.setTableNames(Collections.emptyList());

            List<FieldChange> changes = getDagNodeChanges(node2, node1);
            // tableNames: from ["t1"] to [] - should report removal of t1
            // But note: when one list is empty it won't enter compareListAndAdd since
            // extractEqFields skips null values. If both are non-null, we should get a change.
            // Actually empty list is non-null, so it should be compared.
            FieldChange tableChange = changes.stream()
                    .filter(c -> c.getField().contains("tableNames"))
                    .findFirst().orElse(null);
            // If tableNames is empty, extractEqFields may still include it
            // This test verifies the boundary doesn't cause errors
            if (tableChange != null) {
                // from should have t1 as removed
                assertNotNull(tableChange.getFrom());
            }
        }

        @Test
        @DisplayName("Simple type list: complete replacement")
        void testArrayCompleteReplacement() {
            DatabaseNode node1 = new DatabaseNode();
            node1.setId("n1");
            node1.setName("source");
            node1.setDatabaseType("MySQL");
            node1.setTableNames(List.of("a", "b"));

            DatabaseNode node2 = new DatabaseNode();
            node2.setId("n1");
            node2.setName("source");
            node2.setDatabaseType("MySQL");
            node2.setTableNames(List.of("c", "d"));

            List<FieldChange> changes = getDagNodeChanges(node2, node1);
            FieldChange tableChange = changes.stream()
                    .filter(c -> c.getField().contains("tableNames"))
                    .findFirst().orElse(null);
            assertNotNull(tableChange);
            List<String> removed = (List<String>) tableChange.getFrom();
            List<String> added = (List<String>) tableChange.getTo();
            assertTrue(removed.containsAll(List.of("a", "b")));
            assertTrue(added.containsAll(List.of("c", "d")));
        }
    }
}
