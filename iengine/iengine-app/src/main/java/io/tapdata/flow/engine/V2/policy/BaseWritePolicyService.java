package io.tapdata.flow.engine.V2.policy;

import com.tapdata.tm.commons.task.dto.TaskDto;

/**
 * @author samuel
 * @Description
 * @create 2024-07-09 11:00
 **/
public abstract class BaseWritePolicyService implements WritePolicyService {
	protected TaskDto taskDto;

	public BaseWritePolicyService(TaskDto taskDto) {
		this.taskDto = taskDto;
	}
}
