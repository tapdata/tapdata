package io.tapdata.async.master;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.Container;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.tapdata.async.master.ParallelWorkerStateListener.*;
import static io.tapdata.async.master.QueueWorkerStateListener.STATE_IDLE;
import static io.tapdata.async.master.QueueWorkerStateListener.STATE_LONG_IDLE;
import static io.tapdata.async.master.QueueWorkerStateListener.STATE_NONE;
import static io.tapdata.async.master.QueueWorkerStateListener.STATE_STOPPED;
import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * @author aplomb
 */
public class AsyncParallelWorkerImpl implements AsyncParallelWorker {
	private static final String TAG = AsyncParallelWorkerImpl.class.getSimpleName();
	private final ExecutorsManager executorsManager;
	private ParallelWorkerStateListener parallelWorkerStateListener;
	private ScheduledFuture<?> longIdleScheduleFuture;
	private final AtomicInteger state = new AtomicInteger(STATE_NONE);

	private String id;
	private int parallelCount;
	@Bean
	private AsyncMaster asyncMaster;
	private final Map<String, AsyncQueueWorker> runningQueueWorkers = Collections.synchronizedMap(new LinkedHashMap<>());
//	private final AtomicInteger runningCount = new AtomicInteger(0);
	private final List<Container<JobContext, AsyncQueueWorker>> pendingQueueWorkers = new CopyOnWriteArrayList<>();
	private final AtomicBoolean stopped = new AtomicBoolean(false);
	private final AtomicBoolean started = new AtomicBoolean(false);
	public AsyncParallelWorkerImpl(String id, int parallelCount) {
		this.id = id;
		this.parallelCount = parallelCount;
		executorsManager = ExecutorsManager.getInstance();
		changeState(STATE_NONE, STATE_IDLE, null, false);
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public synchronized AsyncQueueWorker job(String queueWorkerId, JobContext jobContext, Consumer<AsyncQueueWorker> consumer) {
		if(stopped.get())
			return null;
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker(queueWorkerId, false);
		if (consumer != null) {
			consumer.accept(asyncQueueWorker);
		}

		if(state.get() == STATE_RUNNING && runningQueueWorkers.size() < parallelCount) {
			if(!pendingQueueWorkers.isEmpty()) {
				pendingQueueWorkers.add(new Container<>(jobContext, asyncQueueWorker));
				startImmediately();
			} else {
				AsyncQueueWorker old = runningQueueWorkers.putIfAbsent(queueWorkerId, asyncQueueWorker);
				if(old == null) {
					startAsyncQueueWorker(jobContext, asyncQueueWorker);
				} else {
					TapLogger.warn(TAG, "queueWorkerId {} already exists", queueWorkerId);
				}
			}
		} else {
			pendingQueueWorkers.add(new Container<>(jobContext, asyncQueueWorker));
		}
		return asyncQueueWorker;
	}

	@Override
	public AsyncQueueWorker job(JobContext jobContext, Consumer<AsyncQueueWorker> consumer) {
		return job(UUID.randomUUID().toString(), jobContext, consumer);
	}

	@Override
	public void setParallelWorkerStateListener(ParallelWorkerStateListener listener) {
		parallelWorkerStateListener = listener;
	}

	private synchronized boolean changeState(int fromState, int toState, List<Integer> ignoreFromComparePossibleStates, boolean scheduleForLongIdle) {
		boolean result = false;
		if(ignoreFromComparePossibleStates == null) {
			result = state.compareAndSet(fromState, toState);
			if(result && parallelWorkerStateListener != null) {
				CommonUtils.ignoreAnyError(() -> parallelWorkerStateListener.stateChanged(id, fromState, toState), TAG);
			}
		} else {
			if(ignoreFromComparePossibleStates.isEmpty() || ignoreFromComparePossibleStates.contains(state.get())) {
				result = true;
				state.set(toState);
				if(parallelWorkerStateListener != null) {
					CommonUtils.ignoreAnyError(() -> parallelWorkerStateListener.stateChanged(id, fromState, toState), TAG);
				}
			}
		}
		if(result) {
			if(toState != STATE_IDLE) {
				if(longIdleScheduleFuture != null) {
					longIdleScheduleFuture.cancel(true);
					longIdleScheduleFuture = null;
				}
			} else if(scheduleForLongIdle) { //result && toState == STATE_IDLE
				if(longIdleScheduleFuture != null) {
					longIdleScheduleFuture.cancel(true);
					longIdleScheduleFuture = null;
				}
				longIdleScheduleFuture = executorsManager.getScheduledExecutorService().schedule(() -> {
					changeState(STATE_IDLE, STATE_LONG_IDLE, null, false);
				}, 1, TimeUnit.SECONDS);
			}
		}
		return result;
	}

	@Override
	public void stop() {
		Map<String, AsyncQueueWorker> theRunningQueueWorkers = null;
		List<Container<JobContext, AsyncQueueWorker>> thePendingQueueWorkers = null;
		synchronized (this) {
			if(stopped.compareAndSet(false, true)) {
				changeState(state.get(), STATE_STOPPED, Collections.EMPTY_LIST, false);
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
		if(state.get() == STATE_RUNNING && worker != null && !pendingQueueWorkers.isEmpty() && !stopped.get()) {
			startImmediately();
		} else if(pendingQueueWorkers.isEmpty() && runningQueueWorkers.isEmpty()) {
			changeState(STATE_RUNNING, STATE_IDLE, null, true);
		}
	}

	@Override
	public void start() {
		if(changeState(state.get(), STATE_RUNNING, list(STATE_NONE, STATE_IDLE, STATE_LONG_IDLE), false)) {
			startImmediately();
		}
	}
	private synchronized void startImmediately() {
		while(!pendingQueueWorkers.isEmpty() && !stopped.get() && runningQueueWorkers.size() < parallelCount) {
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
					pendingQueueWorkers.add(0, container);
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
