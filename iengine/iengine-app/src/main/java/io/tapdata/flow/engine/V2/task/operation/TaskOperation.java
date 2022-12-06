package io.tapdata.flow.engine.V2.task.operation;

import io.tapdata.flow.engine.V2.task.OpType;

/**
 * @author samuel
 * @Description
 * @create 2022-11-21 11:43
 **/
public abstract class TaskOperation {
	private final OpType opType;

	public TaskOperation(OpType opType) {
		this.opType = opType;
	}

	public OpType getOpType() {
		return opType;
	}

	abstract public String getTaskId();

	@Override
	public String toString() {
		return "TaskOperation{" +
				", opType=" + opType +
				'}';
	}
}
