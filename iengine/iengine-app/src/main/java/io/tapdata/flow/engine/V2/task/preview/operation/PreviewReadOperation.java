package io.tapdata.flow.engine.V2.task.preview.operation;

import io.tapdata.pdk.apis.entity.TapAdvanceFilter;

import java.util.StringJoiner;

/**
 * @author samuel
 * @Description
 * @create 2024-09-25 11:26
 **/
public class PreviewReadOperation extends PreviewOperation {
	private static final int TYPE = 100;
	protected String sourceNodeId;
	protected TapAdvanceFilter tapAdvanceFilter;

	public PreviewReadOperation(String sourceNodeId, int queueLimit) {
		super(TYPE);
		this.sourceNodeId = sourceNodeId;
	}

	public String getSourceNodeId() {
		return sourceNodeId;
	}

	public void setSourceNodeId(String sourceNodeId) {
		this.sourceNodeId = sourceNodeId;
	}

	public TapAdvanceFilter getTapAdvanceFilter() {
		return tapAdvanceFilter;
	}

	public void setTapAdvanceFilter(TapAdvanceFilter tapAdvanceFilter) {
		this.tapAdvanceFilter = tapAdvanceFilter;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", PreviewReadOperation.class.getSimpleName() + "[", "]")
				.add("sourceNodeId='" + sourceNodeId + "'")
				.add("tapAdvanceFilter=" + tapAdvanceFilter)
				.toString();
	}
}
