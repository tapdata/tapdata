package io.tapdata.modules.api.async.master;

import java.util.List;

/**
 * @author aplomb
 */
public interface AsyncParallelWorker extends AsyncWorker {
	String getId();
	<T> AsyncQueueWorker start(String queueWorkerId, JobContext jobContext);
	List<AsyncQueueWorker> runningQueueWorkers();
	List<String> completedIds();
	List<AsyncQueueWorker> pendingQueueWorkers();
}
