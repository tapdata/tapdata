package io.tapdata.flow.engine.V2.task.preview;

import io.tapdata.flow.engine.V2.task.preview.operation.PreviewOperation;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author samuel
 * @Description
 * @create 2024-09-25 11:49
 **/
public class PreviewReadOperationQueue {
	private final Map<String, BlockingQueue<PreviewOperation>> nodeOperationQueueMap;
	private final int queueLimit;

	public PreviewReadOperationQueue(int queueLimit) {
		this.nodeOperationQueueMap = new ConcurrentHashMap<>();
		this.queueLimit = queueLimit;
	}

	public void addOperation(String nodeId, PreviewOperation previewOperation) {
		BlockingQueue<PreviewOperation> queue = nodeOperationQueueMap.computeIfAbsent(nodeId, k -> new ArrayBlockingQueue<>(queueLimit + 10));
		try {
			queue.put(previewOperation);
		} catch (InterruptedException ignored) {
			// do nothing
			Thread.currentThread().interrupt();
		}
	}

	public PreviewOperation poll(String nodeId) {
		BlockingQueue<PreviewOperation> queue = nodeOperationQueueMap.computeIfAbsent(nodeId, k -> new ArrayBlockingQueue<>(queueLimit + 10));
		return queue.poll();
	}

	public PreviewOperation poll(String nodeId, long timeout, TimeUnit timeUnit) throws InterruptedException {
		BlockingQueue<PreviewOperation> queue = nodeOperationQueueMap.computeIfAbsent(nodeId, k -> new ArrayBlockingQueue<>(queueLimit + 10));
		return queue.poll(timeout, timeUnit);
	}
}
