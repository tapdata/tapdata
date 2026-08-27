package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataDqlRecoveryEvent;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Resolves and injects recovery events at a task DAG's source boundary.
 *
 * <p>The runtime source map is deliberately keyed by node id, while the DAG
 * remains the authority for deciding which nodes are legal injection points.
 * A node that is merely present in the runtime map, especially a target node,
 * can never become a recovery entry point.</p>
 */
public final class DqlSourceBoundaryInjector implements DqlReplaySourceNode {
    private final DAG dag;
    private final Map<String, DqlReplaySourceNode> runtimeSourceBoundaries;

    public DqlSourceBoundaryInjector(DAG dag,
                                     Map<String, ? extends DqlReplaySourceNode> runtimeSourceBoundaries) {
        this.dag = Objects.requireNonNull(dag, "task DAG must not be null");
        Objects.requireNonNull(runtimeSourceBoundaries, "runtime source boundaries must not be null");
        this.runtimeSourceBoundaries = Collections.unmodifiableMap(
                runtimeSourceBoundaries.entrySet().stream()
                        .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue))
        );
    }

    /**
     * Returns the only registered source boundary. Multiple source DAG roots
     * are rejected until an explicit table-to-source routing contract exists;
     * silently choosing one would lose or misroute records.
     */
    public DqlReplaySourceNode sourceBoundary() {
        List<Node> sourceNodes = sourceNodes();
        List<Node> registeredSources = sourceNodes.stream()
                .filter(node -> runtimeSourceBoundaries.containsKey(node.getId()))
                .toList();
        if (registeredSources.isEmpty()) {
            if (sourceNodes.isEmpty()) {
                throw new IllegalStateException("DQL recovery source boundary cannot be resolved from the task DAG");
            }
            throw new IllegalStateException(
                    "DQL recovery source boundary is unavailable for DAG source node "
                            + sourceNodes.stream().map(Node::getId).collect(Collectors.joining(","))
            );
        }
        if (registeredSources.size() > 1) {
            throw new IllegalStateException(
                    "DQL recovery source boundary is ambiguous for DAG source nodes "
                            + registeredSources.stream().map(Node::getId).toList()
            );
        }
        return runtimeSourceBoundaries.get(registeredSources.get(0).getId());
    }

    public String sourceNodeId() {
        DqlReplaySourceNode boundary = sourceBoundary();
        return sourceNodes().stream()
                .filter(node -> runtimeSourceBoundaries.get(node.getId()) == boundary)
                .map(Node::getId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "DQL recovery source boundary cannot be resolved from the task DAG"));
    }

    @Override
    public void enqueue(TapdataDqlRecoveryEvent event) {
        if (event == null || !event.isDataEvent()) {
            throw new IllegalArgumentException("DQL source boundary accepts DATA recovery events only");
        }
        sourceBoundary().enqueue(event);
    }

    private List<Node> sourceNodes() {
        List<Node> sourceNodes = dag.getSourceNodes();
        if (sourceNodes == null) {
            return Collections.emptyList();
        }
        List<Node> dataSources = new ArrayList<>();
        for (Node node : sourceNodes) {
            if (node instanceof DataParentNode && node.getId() != null) {
                dataSources.add(node);
            }
        }
        return dataSources;
    }
}
