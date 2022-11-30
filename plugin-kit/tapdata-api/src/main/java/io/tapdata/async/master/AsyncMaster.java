package io.tapdata.async.master;

public interface AsyncMaster {
	AsyncJobChain createAsyncJobChain();
	AsyncQueueWorker createAsyncQueueWorker(String id);

	AsyncQueueWorker createAsyncQueueWorker(String id, boolean globalUniqueId);

	AsyncQueueWorker destroyAsyncQueueWorker(String id);

	AsyncParallelWorker createAsyncParallelWorker(String id, int parallelCount);
	AsyncParallelWorker createAsyncParallelWorker(String id, int parallelCount, boolean globalUniqueId);

	AsyncParallelWorker destroyAsyncParallelWorker(String id);
}
