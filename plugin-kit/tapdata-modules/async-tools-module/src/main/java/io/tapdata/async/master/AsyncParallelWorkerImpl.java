package io.tapdata.async.master;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.Container;
import io.tapdata.modules.api.async.master.*;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author aplomb
 */
public class AsyncParallelWorkerImpl implements AsyncParallelWorker {
	private static final String TAG = AsyncParallelWorkerImpl.class.getSimpleName();
	private String id;
	private int parallelCount;
	@Bean
	private AsyncMaster asyncMaster;
	private final Map<String, AsyncQueueWorker> runningQueueWorkers = Collections.synchronizedMap(new LinkedHashMap<>());
//	private final AtomicInteger runningCount = new AtomicInteger(0);
	private final List<Container<JobContext, AsyncQueueWorker>> pendingQueueWorkers = new CopyOnWriteArrayList<>();
	public AsyncParallelWorkerImpl(String id, int parallelCount) {
		this.id = id;
		this.parallelCount = parallelCount;
	}

	@Override
	public String getId() {
		return null;
	}

	@Override
	public synchronized AsyncQueueWorker start(String queueWorkerId, JobContext jobContext) {
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker(queueWorkerId);
		if(runningQueueWorkers.size() < parallelCount) {
			AsyncQueueWorker old = runningQueueWorkers.putIfAbsent(queueWorkerId, asyncQueueWorker);
			if(old == null) {
				startAsyncQueueWorker(jobContext, asyncQueueWorker);
			} else {
				TapLogger.warn(TAG, "queueWorkerId {} already exists", queueWorkerId);
			}
		} else {
			pendingQueueWorkers.add(new Container<>(jobContext, asyncQueueWorker));
		}
		return asyncQueueWorker;
	}

	private void startAsyncQueueWorker(JobContext jobContext, AsyncQueueWorker asyncQueueWorker) {
		asyncQueueWorker.setQueueWorkerStateListener((id, fromState, toState) -> {
			if(toState == QueueWorkerStateListener.STATE_STOPPED) {
				executePendingWorker(id);
			}
		});
		asyncQueueWorker.start(jobContext);
	}

	private synchronized void executePendingWorker(String id) {
		AsyncQueueWorker worker = runningQueueWorkers.remove(id);
		if(worker != null && !pendingQueueWorkers.isEmpty()) {
			Container<JobContext, AsyncQueueWorker> container = pendingQueueWorkers.remove(0);
			if(container != null) {
				if(runningQueueWorkers.size() < parallelCount) {
					AsyncQueueWorker asyncQueueWorker = container.getP();
					AsyncQueueWorker old = runningQueueWorkers.putIfAbsent(asyncQueueWorker.getId(), asyncQueueWorker);
					if(old == null) {
						startAsyncQueueWorker(container.getT(), asyncQueueWorker);
					} else {
						TapLogger.warn(TAG, "queueWorkerId {} already exists", asyncQueueWorker.getId());
					}
				} else {
					pendingQueueWorkers.add(container);
				}
			}
		}
	}

	@Override
	public List<AsyncQueueWorker> runningQueueWorkers() {
		return null;
	}

	@Override
	public List<String> completedIds() {
		return null;
	}

	@Override
	public List<AsyncQueueWorker> pendingQueueWorkers() {
		return null;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getParallelCount() {
		return parallelCount;
	}

	public void setParallelCount(int parallelCount) {
		this.parallelCount = parallelCount;
	}
}
