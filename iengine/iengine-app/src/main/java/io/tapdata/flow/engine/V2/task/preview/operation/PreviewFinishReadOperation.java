package io.tapdata.flow.engine.V2.task.preview.operation;

import java.util.StringJoiner;

/**
 * @author samuel
 * @Description
 * @create 2024-09-25 18:15
 **/
public class PreviewFinishReadOperation extends PreviewOperation {
	private static final int TYPE = 400;
	private boolean isLast = false;

	public PreviewFinishReadOperation() {
		super(TYPE);
	}

	public static PreviewFinishReadOperation create() {
		return new PreviewFinishReadOperation();
	}

	public PreviewFinishReadOperation last(boolean isLast) {
		this.isLast = isLast;
		return this;
	}

	public boolean isLast() {
		return isLast;
	}

	public void setLast(boolean last) {
		isLast = last;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", PreviewFinishReadOperation.class.getSimpleName() + "[", "]")
				.add("isLast=" + isLast)
				.toString();
	}
}
