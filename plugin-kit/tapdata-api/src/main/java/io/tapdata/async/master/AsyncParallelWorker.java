package io.tapdata.async.master;

import io.tapdata.entity.utils.Container;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author aplomb
 */
public interface AsyncParallelWorker extends AsyncWorker {
	String getId();
	AsyncQueueWorker job(String queueWorkerId, JobContext jobContext, Consumer<AsyncQueueWorker> consumer);
	AsyncQueueWorker job(JobContext jobContext, Consumer<AsyncQueueWorker> consumer);
	void setParallelWorkerStateListener(ParallelWorkerStateListener listener);
	int getState();
	void stop();

	void start();

	Collection<AsyncQueueWorker> runningQueueWorkers();
	List<Container<JobContext, AsyncQueueWorker>> pendingQueueWorkers();
}
