package io.tapdata.async.master;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.Container;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

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
	private final AtomicBoolean stopped = new AtomicBoolean(false);
	public AsyncParallelWorkerImpl(String id, int parallelCount) {
		this.id = id;
		this.parallelCount = parallelCount;
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public synchronized AsyncQueueWorker start(String queueWorkerId, JobContext jobContext, Consumer<AsyncQueueWorker> consumer) {
		if(stopped.get())
			return null;
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker(queueWorkerId, false);
		if (consumer != null) {
			consumer.accept(asyncQueueWorker);
		}

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

	@Override
	public void stop() {
		Map<String, AsyncQueueWorker> theRunningQueueWorkers = null;
		List<Container<JobContext, AsyncQueueWorker>> thePendingQueueWorkers = null;
		synchronized (this) {
			if(stopped.compareAndSet(false, true)) {
				theRunningQueueWorkers = runningQueueWorkers;
				thePendingQueueWorkers = pendingQueueWorkers;
			}
		}
		if(theRunningQueueWorkers != null) {
			for(AsyncQueueWorker asyncQueueWorker : theRunningQueueWorkers.values()) {
				CommonUtils.ignoreAnyError(asyncQueueWorker::stop, TAG);
			}
		}
		if(thePendingQueueWorkers != null) {
			for(Container<JobContext, AsyncQueueWorker> container : thePendingQueueWorkers) {
				CommonUtils.ignoreAnyError(() -> container.getP().stop(), TAG);
			}
		}
	}
	private void startAsyncQueueWorker(JobContext jobContext, AsyncQueueWorker asyncQueueWorker) {
		asyncQueueWorker.setQueueWorkerStateListener((id, fromState, toState) -> {
			if(fromState == QueueWorkerStateListener.STATE_RUNNING && toState == QueueWorkerStateListener.STATE_IDLE) {
//				asyncQueueWorker.stop();
				removeRunningToExecutePendingWorker(id);
			}
		});
		asyncQueueWorker.start(jobContext);
	}

	private synchronized void removeRunningToExecutePendingWorker(String id) {
		AsyncQueueWorker worker = runningQueueWorkers.remove(id); //this worker is already in stopped state.
		if(worker != null)
			CommonUtils.ignoreAnyError(worker::stop, TAG);
		if(worker != null && !pendingQueueWorkers.isEmpty() && !stopped.get()) {
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
	public Collection<AsyncQueueWorker> runningQueueWorkers() {
		return runningQueueWorkers.values();
	}

	@Override
	public List<String> completedIds() {
		return null;
	}

	@Override
	public List<Container<JobContext, AsyncQueueWorker>> pendingQueueWorkers() {
		return pendingQueueWorkers;
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
