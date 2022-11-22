package io.tapdata.modules.api.async.master;

public interface AsyncMaster {
	AsyncJobChain createAsyncJobChain();
	AsyncQueueWorker createAsyncQueueWorker(String id);

	AsyncQueueWorker destroyAsyncQueueWorker(String id);

	AsyncParallelWorker createAsyncParallelWorker(String id, int parallelCount);
	void start();
}
