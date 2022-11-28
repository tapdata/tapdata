package io.tapdata.modules.api.async.master;

import io.tapdata.entity.utils.Container;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author aplomb
 */
public interface AsyncParallelWorker extends AsyncWorker {
	String getId();
	AsyncQueueWorker start(String queueWorkerId, JobContext jobContext, Consumer<AsyncQueueWorker> consumer);

	Collection<AsyncQueueWorker> runningQueueWorkers();
	List<String> completedIds();
	List<Container<JobContext, AsyncQueueWorker>> pendingQueueWorkers();
}
