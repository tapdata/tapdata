package io.tapdata.flow.engine.V2.task.preview.operation;

import io.tapdata.flow.engine.V2.task.preview.tasklet.MergeTableLoopProperty;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;

import java.util.StringJoiner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

/**
 * @author samuel
 * @Description
 * @create 2024-09-30 14:50
 **/
public class PreviewMergeReadOperation extends PreviewOperation {
	private static final int TYPE = 101;
	protected String sourceNodeId;
	protected MergeTableLoopProperty mergeTableLoopProperty;
	protected TapAdvanceFilter tapAdvanceFilter;
	protected final BlockingQueue<MergeReadData> replyDataQueue;
	protected CountDownLatch mergeNodeReceived = new CountDownLatch(1);

	public PreviewMergeReadOperation(String sourceNodeId, MergeTableLoopProperty mergeTableLoopProperty, int queueLimit) {
		super(TYPE);
		this.mergeTableLoopProperty = mergeTableLoopProperty;
		this.replyDataQueue = new ArrayBlockingQueue<>(queueLimit);
		this.sourceNodeId = sourceNodeId;
	}

	public MergeReadData replyData() {
		return replyDataQueue.poll();
	}

	public void replyData(MergeReadData data) throws InterruptedException {
		replyDataQueue.put(data);
	}

	public String getSourceNodeId() {
		return sourceNodeId;
	}

	public BlockingQueue<MergeReadData> getReplyDataQueue() {
		return replyDataQueue;
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

	public MergeTableLoopProperty getMergeTableLoopProperty() {
		return mergeTableLoopProperty;
	}

	public void setMergeTableLoopProperty(MergeTableLoopProperty mergeTableLoopProperty) {
		this.mergeTableLoopProperty = mergeTableLoopProperty;
	}

	public CountDownLatch getMergeNodeReceived() {
		return mergeNodeReceived;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", PreviewMergeReadOperation.class.getSimpleName() + "[", "]")
				.add("sourceNodeId='" + sourceNodeId + "'")
				.add("mergeTableLoopProperty=" + mergeTableLoopProperty)
				.add("tapAdvanceFilter=" + tapAdvanceFilter)
				.add("replyDataQueue=" + replyDataQueue)
				.add("mergeNodeReceived=" + mergeNodeReceived.getCount())
				.toString();
	}
}
