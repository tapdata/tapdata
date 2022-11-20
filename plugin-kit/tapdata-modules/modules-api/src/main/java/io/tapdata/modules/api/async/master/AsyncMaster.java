package io.tapdata.modules.api.async.master;

public interface AsyncMaster {
	<T> AsyncJobChain<T> createAsyncJobChain();
	AsyncQueueWorker createAsyncQueueWorker(String id);
	AsyncParallelWorker createAsyncParallelWorker(String id, int parallelCount);
}
