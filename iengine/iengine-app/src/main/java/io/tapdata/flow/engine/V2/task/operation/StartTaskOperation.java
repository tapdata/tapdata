package io.tapdata.flow.engine.V2.task.operation;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.task.OpType;

/**
 * @author samuel
 * @Description
 * @create 2022-11-21 14:38
 **/
public class StartTaskOperation extends TaskOperation {
	private TaskDto taskDto;

	public StartTaskOperation(OpType opType) {
		super(opType);
	}

	public static StartTaskOperation create() {
		return new StartTaskOperation(OpType.START);
	}

	public StartTaskOperation taskDto(TaskDto taskDto) {
		this.taskDto = taskDto;
		return this;
	}

	public TaskDto getTaskDto() {
		return taskDto;
	}

	@Override
	public String getTaskId() {
		return taskDto.getId().toHexString();
	}

	@Override
	public String toString() {
		return "StartTaskOperation{" +
				"taskDto=" + taskDto +
				"} " + super.toString();
	}
}
