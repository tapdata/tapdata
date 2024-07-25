package io.tapdata.flow.engine.V2.policy;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;

/**
 * @author samuel
 * @Description
 * @create 2024-07-09 11:00
 **/
public abstract class NodeWritePolicyService extends BaseWritePolicyService {
	protected Node<?> node;

	public NodeWritePolicyService(TaskDto taskDto, Node<?> node) {
		super(taskDto);
		this.node = node;
	}
}
