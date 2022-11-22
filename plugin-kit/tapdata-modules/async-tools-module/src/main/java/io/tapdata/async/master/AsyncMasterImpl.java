package io.tapdata.async.master;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.async.master.AsyncJobChain;
import io.tapdata.modules.api.async.master.AsyncMaster;
import io.tapdata.modules.api.async.master.AsyncParallelWorker;
import io.tapdata.modules.api.async.master.AsyncQueueWorker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Implementation(AsyncMaster.class)
public class AsyncMasterImpl implements AsyncMaster {
	private Map<String, AsyncQueueWorker> asyncQueueWorkerMap = new ConcurrentHashMap<>();
	private Map<String, AsyncParallelWorker> asyncParallelWorkerMap = new ConcurrentHashMap<>();

	@Override
	public AsyncJobChain createAsyncJobChain() {
		return new AsyncJobChainImpl();
	}

	@Override
	public AsyncQueueWorker createAsyncQueueWorker(String id) {
		return asyncQueueWorkerMap.computeIfAbsent(id, id1 -> {
			AsyncQueueWorkerImpl asyncQueueWorker = new AsyncQueueWorkerImpl(id);
			InstanceFactory.injectBean(asyncQueueWorker);
			return asyncQueueWorker;
		});
	}

	@Override
	public AsyncQueueWorker destroyAsyncQueueWorker(String id) {
		AsyncQueueWorker asyncQueueWorker = asyncQueueWorkerMap.remove(id);
		if(asyncQueueWorker != null) {
			asyncQueueWorker.stop();
		}
		return asyncQueueWorker;
	}

	@Override
	public AsyncParallelWorker createAsyncParallelWorker(String id, int parallelCount) {
		return asyncParallelWorkerMap.computeIfAbsent(id, id1 -> {
			AsyncParallelWorkerImpl asyncParallelWorker = new AsyncParallelWorkerImpl(id, parallelCount);
			InstanceFactory.injectBean(asyncParallelWorker);
			return asyncParallelWorker;
		});
	}

	@Override
	public void start() {

	}
}
