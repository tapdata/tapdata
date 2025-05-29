package io.tapdata.flow.engine.V2.util;

import com.google.common.collect.Lists;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.process.UnionProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
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
					result.addAll(predecessors(nodePredecessor, nodeFilter, null));
				}
			}
		}
		return result;
	}

	public static List<Node> findMergeNode(TaskDto taskDto) {
		if (null == taskDto || null == taskDto.getDag()) {
			return Collections.emptyList();
		}
		List<Node> nodes = taskDto.getDag().getNodes();
		if (CollectionUtils.isEmpty(nodes)) {
			return Collections.emptyList();
		}
		return nodes.stream().filter(n -> n instanceof MergeTableNode
				|| n instanceof JoinProcessorNode
				|| n instanceof UnionProcessorNode).collect(Collectors.toList());
	}

	public static List<Node> findNodes(TaskDto taskDto, Class<?>... matchClazz) {
		if (null == taskDto || null == taskDto.getDag() || null == matchClazz || matchClazz.length == 0) {
			return Collections.emptyList();
		}
		List<Node> nodes = taskDto.getDag().getNodes();
		if (CollectionUtils.isEmpty(nodes)) {
			return Collections.emptyList();
		}
		return nodes.stream().filter(n -> {
			for (Class<?> clazz : matchClazz) {
				if (clazz.isInstance(n)) {
					return true;
				}
			}
			return false;
		}).collect(Collectors.toList());
	}
}
