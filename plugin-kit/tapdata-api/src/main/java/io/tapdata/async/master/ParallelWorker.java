package io.tapdata.async.master;

import io.tapdata.entity.utils.Container;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author aplomb
 */
public interface ParallelWorker extends AsyncWorker {
	String getId();
	QueueWorker job(String queueWorkerId, JobContext jobContext, Consumer<QueueWorker> consumer);
	QueueWorker job(JobContext jobContext, Consumer<QueueWorker> consumer);
	void setParallelWorkerStateListener(ParallelWorkerStateListener listener);
	int getState();
	void stop();

	void start();
	void start(boolean startOnCurrentThread);

	Collection<QueueWorker> runningQueueWorkers();
	List<Container<JobContext, QueueWorker>> pendingQueueWorkers();

	void finished(Runnable finishedRunnable);
}
