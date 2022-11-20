package io.tapdata.async.master;

import io.tapdata.modules.api.async.master.AsyncJobChain;
import io.tapdata.modules.api.async.master.AsyncMaster;
import io.tapdata.modules.api.async.master.AsyncParallelWorker;
import io.tapdata.modules.api.async.master.AsyncQueueWorker;

public class AsyncMasterImpl implements AsyncMaster {

	@Override
	public <T> AsyncJobChain createAsyncJobChain() {
		return null;
	}

	@Override
	public AsyncQueueWorker createAsyncQueueWorker(String id) {
		return null;
	}

	@Override
	public AsyncParallelWorker createAsyncParallelWorker(String id, int parallelCount) {
		return null;
	}
}
