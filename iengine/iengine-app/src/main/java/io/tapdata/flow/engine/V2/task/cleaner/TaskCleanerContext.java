package io.tapdata.flow.engine.V2.task.cleaner;

import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.flow.engine.V2.task.OpType;

/**
 * @author samuel
 * @Description
 * @create 2022-10-14 10:43
 **/
public class TaskCleanerContext {
	private OpType opType;
	private String taskId;
	private ClientMongoOperator clientMongoOperator;

	public TaskCleanerContext(String taskId, ClientMongoOperator clientMongoOperator) {
		this.taskId = taskId;
		this.clientMongoOperator = clientMongoOperator;
	}

	public TaskCleanerContext opType(OpType opType) {
		this.opType = opType;
		return this;
	}

	public OpType getOpType() {
		return opType;
	}

	public String getTaskId() {
		return taskId;
	}

	public ClientMongoOperator getClientMongoOperator() {
		return clientMongoOperator;
	}
}
