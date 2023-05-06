package io.tapdata.flow.engine.V2.task;

import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-10-13 17:44
 **/
public enum OpType {
	RESET("reset", "io.tapdata.flow.engine.V2.task.cleaner.TaskResetCleaner"),
	DELETE("delete", "io.tapdata.flow.engine.V2.task.cleaner.TaskDeleteCleaner"),
	START("start", ""),
	STOP("stop", "");
	private String op;
	private String implementClass;

	OpType(String op, String implementClass) {
		this.op = op;
		this.implementClass = implementClass;
	}

	public String getOp() {
		return op;
	}

	public String getImplementClass() {
		return implementClass;
	}

	private static Map<String, OpType> map;

	static {
		map = new HashMap<>();
		for (OpType value : OpType.values()) {
			map.put(value.getOp(), value);
		}
	}

	public static OpType fromOp(String op) {
		return map.get(op);
	}
}
