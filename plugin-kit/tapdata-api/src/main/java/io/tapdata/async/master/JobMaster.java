package io.tapdata.async.master;

public interface JobMaster {
	JobChain createAsyncJobChain();
	QueueWorker createAsyncQueueWorker(String id);

	QueueWorker createAsyncQueueWorker(String id, boolean globalUniqueId);

	QueueWorker destroyAsyncQueueWorker(String id);

	ParallelWorker createAsyncParallelWorker(String id, int parallelCount);
	ParallelWorker createAsyncParallelWorker(String id, int parallelCount, boolean globalUniqueId);

	ParallelWorker destroyAsyncParallelWorker(String id);
}
