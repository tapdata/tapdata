package io.tapdata.flow.engine.V2.util;

import com.google.common.collect.Lists;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.process.UnionProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-03-08 10:58
 **/
public class GraphUtil {
	public static List<Node<?>> successors(Node<?> node, Predicate<Node<?>> nodeFilter) {
		final List<Node<?>> successors = Lists.newArrayList();
		final Queue<Node<?>> queue = new LinkedList<>();
		if (node == null) {
			return Lists.newArrayList();
		}
		List<? extends Node<?>> nodeSuccessors = node.successors();
		if (CollectionUtils.isEmpty(nodeSuccessors)) {
			return Lists.newArrayList();
		}
		nodeSuccessors.forEach(queue::offer);
		while (!queue.isEmpty()) {
			Node<?> cur = queue.poll();
			if (nodeFilter == null || nodeFilter.test(cur)) {
				successors.add(cur);
			} else {
				List<? extends Node<?>> nextNodes = cur.successors();
				if (CollectionUtils.isNotEmpty(nextNodes)) {
					nextNodes.forEach(queue::offer);
				}
			}
		}
		return successors;
	}

	public static List<Node<?>> predecessors(Node<?> node, Predicate<Node<?>> nodeFilter) {
		return predecessors(node, nodeFilter, null);
	}

	public static List<Node<?>> predecessors(Node<?> node, Predicate<Node<?>> nodeFilter, List<Node<?>> predecessors) {
		if (null == predecessors) {
			predecessors = new ArrayList<>();
		}
		if (null == node) {
			return predecessors;
		}
		List<? extends Node<?>> nodePredecessors;
		List<Node<?>> result = new ArrayList<>();
		if (CollectionUtils.isNotEmpty(predecessors)) {
			nodePredecessors = predecessors;
		} else {
			nodePredecessors = node.predecessors();
		}
		for (Node<?> nodePredecessor : nodePredecessors) {
			if (null == nodeFilter) {
				result.add(nodePredecessor);
			} else {
				if (nodeFilter.test(nodePredecessor)) {
					result.add(nodePredecessor);
				} else {
					result.addAll(predecessors(nodePredecessor, nodeFilter, result));
				}
			}
		}
		return result;
	}

	public static List<Node> findMergeNode(TaskDto taskDto) {
		List<Node> nodes = taskDto.getDag().getNodes();
		return nodes.stream().filter(n -> n instanceof MergeTableNode
				|| n instanceof JoinProcessorNode
				|| n instanceof UnionProcessorNode).collect(Collectors.toList());
	}
}
