package io.tapdata.flow.engine.V2.util;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-03-06 22:46
 **/
public class NodeUtil {
	public static String getVertexName(Node<?> node) {
		return String.join("-", node.getName(), node.getId());
	}

	public static List<Node> findAllPreNodes(Node node) {
		if (node == null) {
			return null;
		}

		List<Node> preNodes = new ArrayList<>();
		final List<Node> predecessors = node.predecessors();
		if (CollectionUtils.isNotEmpty(predecessors)) {
			preNodes.addAll(predecessors);
			for (Node predecessor : predecessors) {
				final List<Node> allPreNodes = findAllPreNodes(predecessor);
				if (CollectionUtils.isNotEmpty(allPreNodes)) {
					preNodes.addAll(allPreNodes);
				}
			}
		}
		return preNodes;
	}

	public static List<String> findAllTableName(Node<?> node) {
		List<String> tableNames = new ArrayList<>();
		if (node == null) {
			return tableNames;
		}
		DAG dag = node.getDag();
		if (dag == null) {
			return tableNames;
		}
		List<Node> nodes = dag.getNodes();
		if (CollectionUtils.isEmpty(nodes)) {
			return tableNames;
		}
		String target = traceToTerminal(dag.getEdges());
		for (Node<?> n : nodes) {
			if (n.isDisabled() || (null != target && target.equals(n.getId()))) {
				continue;
			}
			if (n instanceof DatabaseNode databaseNode) {
				tableNames.addAll(databaseNode.getTableNames());
			} else if (n instanceof TableNode tableNode) {
				tableNames.add(tableNode.getTableName());
			}
		}
		return tableNames;
	}

	public static String traceToTerminal(List<Edge> edges) {
		List<String> sources = edges.stream()
				.filter(e -> !e.isDisabled())
				.map(Edge::getSource)
				.distinct()
				.toList();
		return edges.stream()
				.filter(e -> !e.isDisabled())
				.map(Edge::getTarget)
				.distinct()
				.filter(e -> !sources.contains(e))
				.findFirst().orElse(null);
	}
}
