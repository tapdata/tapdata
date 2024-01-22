package io.tapdata.flow.engine.V2.cleaner;

import com.tapdata.tm.commons.dag.Node;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2024-01-03 12:23
 **/
public class BaseTaskCleaner implements ICleaner {

	@Override
	public CleanResult cleanTaskNode(String taskId, String nodeId) {
		return CleanResult.success();
	}

	protected <E extends Node<?>> List<E> findNodes(List<Node> nodes, Class<E> nodeClz) {
		if (CollectionUtils.isEmpty(nodes)) {
			return new ArrayList<>();
		}
		List<E> result = new ArrayList<>();
		for (Node node : nodes) {
			if (nodeClz.isInstance(node)) {
				result.add((E) node);
			}
		}
		return result;
	}
}
