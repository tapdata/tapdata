package io.tapdata.dql.recovery;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.Dag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlRecoveryDagPlannerTest {

    @Test
    void clonesDagAndHidesOnlyNodesOutsideFailedNodeToTargetPath() {
        DAG original = dag(
                edge("source", "failed"),
                edge("failed", "processor"),
                edge("processor", "target"),
                edge("unrelatedSource", "unrelatedTarget")
        );

        DqlRecoveryDagPlanner.Plan plan = DqlRecoveryDagPlanner.plan(original, "failed", "target");

        assertEquals(List.of("failed", "processor", "target"),
                plan.retainedNodeIds().stream().sorted().toList());
        assertFalse(plan.dag().getNode("failed").disabledNode());
        assertFalse(plan.dag().getNode("processor").disabledNode());
        assertFalse(plan.dag().getNode("target").disabledNode());
        assertTrue(plan.dag().getNode("source").disabledNode());
        assertTrue(plan.dag().getNode("unrelatedSource").disabledNode());
        assertTrue(plan.dag().getNode("unrelatedTarget").disabledNode());

        // The persisted task graph is never mutated by planning.
        assertFalse(original.getNode("source").disabledNode());
        assertFalse(original.getNode("failed").disabledNode());
        assertFalse(original.getNode("unrelatedSource").disabledNode());
        assertTrue(plan.hiddenNodes().stream().anyMatch(node -> "source".equals(node.nodeId())));
        assertTrue(plan.hiddenNodes().stream().anyMatch(node -> "unrelatedTarget".equals(node.nodeId())));
    }

    @Test
    void restoresOnlyNodesTemporarilyHiddenAndPreservesExistingDisabledState() {
        DAG original = dag(edge("source", "failed"), edge("failed", "target"));
        Node<?> source = original.getNode("source");
        source.setDisabled(true);
        source.setAttrs(new HashMap<>(Map.of("disabled", true, "custom", "keep")));

        DqlRecoveryDagPlanner.Plan plan = DqlRecoveryDagPlanner.plan(original, "failed", "target");

        assertTrue(plan.hiddenNodes().isEmpty(), "an already disabled source must not be overwritten");
        assertTrue(plan.dag().getNode("source").disabledNode());
        plan.restore();
        assertTrue(plan.dag().getNode("source").disabledNode());
        assertEquals(Map.of("disabled", true, "custom", "keep"), plan.dag().getNode("source").getAttrs());
    }

    @Test
    void rejectsAnUnknownOrUnreachableReplayPath() {
        DAG original = dag(edge("source", "failed"), edge("failed", "target"));

        assertThrows(IllegalArgumentException.class,
                () -> DqlRecoveryDagPlanner.plan(original, "missing", "target"));
        assertThrows(IllegalArgumentException.class,
                () -> DqlRecoveryDagPlanner.plan(original, "failed", "otherTarget"));
    }

    private static DAG dag(Edge... edges) {
        Map<String, Node<?>> nodes = new java.util.LinkedHashMap<>();
        for (Edge edge : edges) {
            nodes.computeIfAbsent(edge.getSource(), DqlRecoveryDagPlannerTest::node);
            nodes.computeIfAbsent(edge.getTarget(), DqlRecoveryDagPlannerTest::node);
        }
        return DAG.build(new Dag(List.of(edges), List.copyOf(nodes.values())));
    }

    private static Edge edge(String source, String target) {
        return new Edge(source, target);
    }

    private static Node<?> node(String id) {
        DatabaseNode node = new DatabaseNode();
        node.setId(id);
        node.setName(id + "-name");
        return node;
    }
}
