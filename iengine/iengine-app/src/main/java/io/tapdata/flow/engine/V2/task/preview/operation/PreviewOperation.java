package io.tapdata.flow.engine.V2.task.preview.operation;

import java.util.StringJoiner;

/**
 * @author samuel
 * @Description
 * @create 2024-09-25 11:23
 **/
public abstract class PreviewOperation {
	private int type;

	public PreviewOperation(int type) {
		this.type = type;
	}

	public int getType() {
		return type;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", PreviewOperation.class.getSimpleName() + "[", "]")
				.add("type=" + type)
				.toString();
	}
}
