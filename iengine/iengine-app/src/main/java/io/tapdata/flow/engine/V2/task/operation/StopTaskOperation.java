package io.tapdata.flow.engine.V2.task.operation;

import io.tapdata.flow.engine.V2.task.OpType;

/**
 * @author samuel
 * @Description
 * @create 2022-11-21 14:41
 **/
public class StopTaskOperation extends TaskOperation {

	private String taskId;

	public StopTaskOperation(OpType opType) {
		super(opType);
	}

	public static StopTaskOperation create() {
		return new StopTaskOperation(OpType.STOP);
	}

	public StopTaskOperation taskId(String taskId) {
		this.taskId = taskId;
		return this;
	}

	@Override
	public String getTaskId() {
		return taskId;
	}

	@Override
	public String toString() {
		return "StopTaskOperation{" +
				"taskId='" + taskId + '\'' +
				"} " + super.toString();
	}
}
