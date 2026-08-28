package io.tapdata.dql.recovery;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.Dag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Builds the isolated DAG used by DQL replay.
 *
 * <p>This class deliberately operates on a deep copy.  A replay must never
 * toggle {@code disabled} on the DAG held by the live TaskDto, otherwise a
 * failed recovery can silently change the next normal task start.</p>
 */
public final class DqlRecoveryDagPlanner {
    public static final String DISABLED_ATTR = "disabled";

    private DqlRecoveryDagPlanner() {
    }

    public static Plan plan(DAG originalDag, String failedNodeId, String targetNodeId) {
        Objects.requireNonNull(originalDag, "originalDag must not be null");
        requireText(failedNodeId, "failedNodeId");

        final DAG replayDag;
        try {
            replayDag = originalDag.clone();
        } catch (CloneNotSupportedException exception) {
            throw new IllegalStateException("failed to clone task DAG for DQL recovery", exception);
        }
        // DAG.clone() serializes only the node/edge DTO. Restore the runtime
        // metadata that node implementations use for task-scoped state and
        // logging; otherwise a temporary replay graph can have a null task
        // id even though its TaskDto has a valid id.
        copyDagMetadata(originalDag, replayDag);

        Map<String, Node<?>> nodes = indexNodes(replayDag);
        if (!nodes.containsKey(failedNodeId)) {
            throw new IllegalArgumentException("DQL recovery failed node does not exist in task DAG: " + failedNodeId);
        }
        if (nodes.get(failedNodeId).disabledNode()) {
            throw new IllegalArgumentException("DQL recovery failed node is disabled: " + failedNodeId);
        }

        Map<String, Set<String>> successors = successors(replayDag.getEdges(), nodes.keySet());
        Map<String, Integer> incomingCounts = incomingCounts(replayDag.getEdges());
        int failedIncomingCount = incomingCounts.getOrDefault(failedNodeId, 0);
        if (failedIncomingCount != 1) {
            throw new IllegalArgumentException(String.format(
                    "DQL recovery failed node %s must have exactly one input, actual input count=%s",
                    failedNodeId, failedIncomingCount));
        }
		Set<String> reachable = reachableFrom(failedNodeId, successors);
		Set<String> retained = new LinkedHashSet<>(reachable);
		// Older Engine builds recorded the currently executing processor as
		// targetNodeId.  If that value equals the failed node while it still has
		// successors, treating it as the replay boundary would drop the actual
		// downstream path.  Keep the metadata for display, but recover all
		// descendants for this legacy shape.
		boolean legacyProcessorTarget = hasText(targetNodeId)
				&& failedNodeId.equals(targetNodeId)
				&& !successors.getOrDefault(failedNodeId, Collections.emptySet()).isEmpty();
		if (hasText(targetNodeId) && !legacyProcessorTarget) {
            if (!nodes.containsKey(targetNodeId)) {
                throw new IllegalArgumentException("DQL recovery target node does not exist in task DAG: " + targetNodeId);
            }
            if (!reachable.contains(targetNodeId)) {
                throw new IllegalArgumentException(String.format(
                        "DQL recovery target node %s is not reachable from failed node %s",
                        targetNodeId, failedNodeId));
            }
            Set<String> canReachTarget = reverseReachable(targetNodeId, replayDag.getEdges());
            retained.retainAll(canReachTarget);
            retained.add(failedNodeId);
        }

        // A replay event contains one upstream payload.  Feeding a node with
        // multiple inputs would silently omit the other input and can produce
        // an invalid join/merge result, so fail closed instead of guessing.
        for (String retainedNodeId : retained) {
            if (!retainedNodeId.equals(failedNodeId)
                    && incomingCounts.getOrDefault(retainedNodeId, 0) > 1) {
                throw new IllegalArgumentException(String.format(
                        "DQL recovery path contains multi-input node %s; replay is unsupported",
                        retainedNodeId));
            }
        }

        List<HiddenNode> hiddenNodes = new ArrayList<>();
        Map<String, NodeSnapshot> snapshots = new HashMap<>();
        for (Node<?> node : replayDag.getNodes()) {
            if (node == null || retained.contains(node.getId())) {
                continue;
            }
            boolean disabledBefore = node.disabledNode();
            if (disabledBefore) {
                continue;
            }
            snapshots.put(node.getId(), NodeSnapshot.capture(node));
            setDisabled(node, true);
            hiddenNodes.add(new HiddenNode(node.getId(), node.getName(), false, true));
        }

        // Rebuild the cloned graph after cutting the original input edge to
        // the failed node.  Node.predecessors()/successors() are backed by
        // DAG.graph, so merely passing a filtered edge list to Jet is not
        // enough: without rebuilding this graph the failed node could still
        // be classified as a source-and-target node and start its connector.
        List<Edge> runtimeEdges = replayDag.getEdges().stream()
                .filter(edge -> retained.contains(edge.getSource()))
                .filter(edge -> retained.contains(edge.getTarget()))
                .filter(edge -> !failedNodeId.equals(edge.getTarget()))
                .toList();
        DAG runtimeDag = DAG.build(new Dag(runtimeEdges, new ArrayList<>(replayDag.getNodes())));
        copyDagMetadata(replayDag, runtimeDag);

        return new Plan(runtimeDag, failedNodeId, targetNodeId, retained, hiddenNodes, snapshots);
    }

    private static void copyDagMetadata(DAG source, DAG target) {
        target.setTaskId(source.getTaskId());
        target.setSyncType(source.getSyncType());
        target.setOwnerId(source.getOwnerId());
    }

    private static Map<String, Node<?>> indexNodes(DAG dag) {
        Map<String, Node<?>> nodes = new HashMap<>();
        if (dag.getNodes() != null) {
            for (Node<?> node : dag.getNodes()) {
                if (node != null && hasText(node.getId())) {
                    nodes.put(node.getId(), node);
                }
            }
        }
        return nodes;
    }

    private static Map<String, Set<String>> successors(List<Edge> edges, Set<String> nodeIds) {
        Map<String, Set<String>> successors = new HashMap<>();
        for (String nodeId : nodeIds) {
            successors.put(nodeId, new LinkedHashSet<>());
        }
        if (edges == null) {
            return successors;
        }
        for (Edge edge : edges) {
            if (edge == null || !nodeIds.contains(edge.getSource()) || !nodeIds.contains(edge.getTarget())) {
                continue;
            }
            successors.computeIfAbsent(edge.getSource(), ignored -> new LinkedHashSet<>()).add(edge.getTarget());
        }
        return successors;
    }

    private static Map<String, Integer> incomingCounts(List<Edge> edges) {
        Map<String, Integer> incomingCounts = new HashMap<>();
        if (edges == null) {
            return incomingCounts;
        }
        for (Edge edge : edges) {
            if (edge == null || edge.getTarget() == null) {
                continue;
            }
            incomingCounts.merge(edge.getTarget(), 1, Integer::sum);
        }
        return incomingCounts;
    }

    private static Set<String> reachableFrom(String nodeId, Map<String, Set<String>> successors) {
        Set<String> reachable = new LinkedHashSet<>();
        ArrayDeque<String> queue = new ArrayDeque<>(Collections.singleton(nodeId));
        while (!queue.isEmpty()) {
            String current = queue.removeFirst();
            if (!reachable.add(current)) {
                continue;
            }
            queue.addAll(successors.getOrDefault(current, Collections.emptySet()));
        }
        return reachable;
    }

    private static Set<String> reverseReachable(String nodeId, List<Edge> edges) {
        Map<String, Set<String>> predecessors = new HashMap<>();
        if (edges != null) {
            for (Edge edge : edges) {
                if (edge == null) {
                    continue;
                }
                predecessors.computeIfAbsent(edge.getTarget(), ignored -> new LinkedHashSet<>()).add(edge.getSource());
            }
        }
        return reachableFrom(nodeId, predecessors);
    }

    private static void setDisabled(Node<?> node, boolean disabled) {
        Map<String, Object> attrs = node.getAttrs() == null
                ? new HashMap<>()
                : new HashMap<>(node.getAttrs());
        attrs.put(DISABLED_ATTR, disabled);
        node.setAttrs(attrs);
        node.setDisabled(disabled);
    }

    private static void requireText(String value, String field) {
        if (!hasText(value)) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    public record HiddenNode(String nodeId,
                             String nodeName,
                             boolean disabledBefore,
                             boolean disabledDuring) {
    }

    public static final class Plan {
        private final DAG dag;
        private final String failedNodeId;
        private final String targetNodeId;
        private final Set<String> retainedNodeIds;
        private final List<HiddenNode> hiddenNodes;
        private final Map<String, NodeSnapshot> snapshots;

        private Plan(DAG dag,
                     String failedNodeId,
                     String targetNodeId,
                     Set<String> retainedNodeIds,
                     List<HiddenNode> hiddenNodes,
                     Map<String, NodeSnapshot> snapshots) {
            this.dag = dag;
            this.failedNodeId = failedNodeId;
            this.targetNodeId = targetNodeId;
            this.retainedNodeIds = Collections.unmodifiableSet(new LinkedHashSet<>(retainedNodeIds));
            this.hiddenNodes = Collections.unmodifiableList(new ArrayList<>(hiddenNodes));
            this.snapshots = new HashMap<>(snapshots);
        }

        public DAG dag() {
            return dag;
        }

        public String failedNodeId() {
            return failedNodeId;
        }

        public String targetNodeId() {
            return targetNodeId;
        }

        public Set<String> retainedNodeIds() {
            return retainedNodeIds;
        }

        public List<HiddenNode> hiddenNodes() {
            return hiddenNodes;
        }

        /** Restores exactly the nodes changed by this plan. */
        public void restore() {
            for (Map.Entry<String, NodeSnapshot> entry : snapshots.entrySet()) {
                Node<?> node = dag.getNode(entry.getKey());
                if (node != null) {
                    entry.getValue().restore(node);
                }
            }
        }
    }

    private record NodeSnapshot(Map<String, Object> attrs, boolean disabled) {
        private static NodeSnapshot capture(Node<?> node) {
            return new NodeSnapshot(node.getAttrs() == null ? null : new HashMap<>(node.getAttrs()), node.isDisabled());
        }

        private void restore(Node<?> node) {
            node.setAttrs(attrs == null ? null : new HashMap<>(attrs));
            node.setDisabled(disabled);
        }
    }
}
