package io.tapdata.flow.engine.V2.util;

import com.tapdata.tm.commons.dag.Node;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

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
}
