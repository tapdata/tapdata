package io.tapdata.async.master;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.reflection.ClassAnnotationManager;
import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Implementation(AsyncMaster.class)
public class AsyncMasterImpl implements AsyncMaster {
	private final Map<String, AsyncQueueWorker> asyncQueueWorkerMap = new ConcurrentHashMap<>();
	private final Map<String, AsyncParallelWorker> asyncParallelWorkerMap = new ConcurrentHashMap<>();

	private final AsyncJobAnnotationHandler asyncJobAnnotationHandler;
	public AsyncMasterImpl() {
		asyncJobAnnotationHandler = new AsyncJobAnnotationHandler();

		ClassAnnotationManager classAnnotationManager = ClassFactory.create(ClassAnnotationManager.class);
		if (classAnnotationManager != null) {
			classAnnotationManager
					.registerClassAnnotationHandler(asyncJobAnnotationHandler);
			String scanPackage = CommonUtils.getProperty("pdk_async_job_scan_package", "io.tapdata,com.tapdata");
			String[] packages = scanPackage.split(",");
			classAnnotationManager.scan(packages, this.getClass().getClassLoader());
		}
	}

	@Override
	public AsyncJobChain createAsyncJobChain() {
		return new AsyncJobChainImpl(asyncJobAnnotationHandler.getAsyncJobMap());
	}

	public AsyncQueueWorker createAsyncQueueWorker(String id) {
		return createAsyncQueueWorker(id, false);
	}

	@Override
	public AsyncQueueWorker createAsyncQueueWorker(String id, boolean globalUniqueId) {
		if(globalUniqueId) {
			return asyncQueueWorkerMap.computeIfAbsent(id, id1 -> {
				AsyncQueueWorkerImpl asyncQueueWorker = new AsyncQueueWorkerImpl(id, asyncJobAnnotationHandler.getAsyncJobMap());
				InstanceFactory.injectBean(asyncQueueWorker);
				return asyncQueueWorker;
			});
		} else {
			AsyncQueueWorkerImpl asyncQueueWorker = new AsyncQueueWorkerImpl(id, asyncJobAnnotationHandler.getAsyncJobMap());
			InstanceFactory.injectBean(asyncQueueWorker);
			return asyncQueueWorker;
		}

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
	public AsyncParallelWorker createAsyncParallelWorker(String id, int parallelCount, boolean globalUniqueId) {
		if(globalUniqueId) {
			return asyncParallelWorkerMap.computeIfAbsent(id, id1 -> {
				AsyncParallelWorkerImpl asyncParallelWorker = new AsyncParallelWorkerImpl(id, parallelCount);
				InstanceFactory.injectBean(asyncParallelWorker);
				return asyncParallelWorker;
			});
		} else {
			AsyncParallelWorkerImpl asyncParallelWorker = new AsyncParallelWorkerImpl(id, parallelCount);
			InstanceFactory.injectBean(asyncParallelWorker);
			return asyncParallelWorker;
		}
	}

	@Override
	public AsyncParallelWorker createAsyncParallelWorker(String id, int parallelCount) {
		return createAsyncParallelWorker(id, parallelCount, false);
	}

	@Override
	public AsyncParallelWorker destroyAsyncParallelWorker(String id) {
		AsyncParallelWorker parallelWorker = asyncParallelWorkerMap.remove(id);
		if(parallelWorker != null) {
			parallelWorker.stop();
		}
		return parallelWorker;
	}

}
