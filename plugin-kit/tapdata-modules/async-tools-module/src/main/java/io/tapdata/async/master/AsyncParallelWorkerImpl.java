package io.tapdata.async.master;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.Container;
import io.tapdata.modules.api.async.master.AsyncErrors;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * @author aplomb
 */
public class AsyncParallelWorkerImpl implements ParallelWorker {
	private static final String TAG = AsyncParallelWorkerImpl.class.getSimpleName();
	private final ExecutorsManager executorsManager;
	private ParallelWorkerStateListener parallelWorkerStateListener;
//	private ScheduledFuture<?> longIdleScheduleFuture;
	private final AtomicInteger state = new AtomicInteger(ParallelWorkerStateListener.STATE_NONE);

	private String id;
	private int parallelCount;
	@Bean
	private JobMaster asyncMaster;
	private final Map<String, QueueWorker> runningQueueWorkers = Collections.synchronizedMap(new LinkedHashMap<>());
//	private final AtomicInteger runningCount = new AtomicInteger(0);
	private final List<Container<JobContext, QueueWorker>> pendingQueueWorkers = new CopyOnWriteArrayList<>();
	private final AtomicBoolean stopped = new AtomicBoolean(false);
	private final AtomicBoolean finished = new AtomicBoolean(false);
	private boolean startOnCurrentThread;
	private Runnable finishedRunnable;

	public AsyncParallelWorkerImpl(String id, int parallelCount) {
		this.id = id;
		this.parallelCount = parallelCount;
		executorsManager = ExecutorsManager.getInstance();
//		changeState(ParallelWorkerStateListener.STATE_NONE, ParallelWorkerStateListener.STATE_IDLE, null, false);
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public synchronized QueueWorker job(String queueWorkerId, JobContext jobContext, Consumer<QueueWorker> consumer) {
		if(stopped.get())
			throw new CoreException(AsyncErrors.PARALLEL_WORKER_STOPPED, "Parallel worker has stopped, {}", id);
		if(finishedRunnable != null)
			throw new CoreException(AsyncErrors.PARALLEL_WORKER_FINISHED, "Parallel worker has finished, {}", id);
		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker(queueWorkerId, false);
		if (consumer != null) {
			consumer.accept(asyncQueueWorker);
		}

		if(state.get() == ParallelWorkerStateListener.STATE_RUNNING && runningQueueWorkers.size() < parallelCount) {
			if(!pendingQueueWorkers.isEmpty()) {
				pendingQueueWorkers.add(new Container<>(jobContext, asyncQueueWorker));
				startImmediately();
			} else {
				QueueWorker old = runningQueueWorkers.putIfAbsent(queueWorkerId, asyncQueueWorker);
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
	public QueueWorker job(JobContext jobContext, Consumer<QueueWorker> consumer) {
		return job(UUID.randomUUID().toString(), jobContext, consumer);
	}

	@Override
	public void setParallelWorkerStateListener(ParallelWorkerStateListener listener) {
		parallelWorkerStateListener = listener;
	}

	@Override
	public int getState() {
		return state.get();
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
//		if(result) {
//			if(toState != ParallelWorkerStateListener.STATE_IDLE) {
//				if(longIdleScheduleFuture != null) {
//					longIdleScheduleFuture.cancel(true);
//					longIdleScheduleFuture = null;
//				}
//			} else if(scheduleForLongIdle) { //result && toState == STATE_IDLE
//				if(longIdleScheduleFuture != null) {
//					longIdleScheduleFuture.cancel(true);
//					longIdleScheduleFuture = null;
//				}
//				longIdleScheduleFuture = executorsManager.getScheduledExecutorService().schedule(() -> {
//					changeState(ParallelWorkerStateListener.STATE_IDLE, ParallelWorkerStateListener.STATE_LONG_IDLE, null, false);
//				}, 1, TimeUnit.SECONDS);
//			}
//		}
		return result;
	}

	@Override
	public void stop() {
		Map<String, QueueWorker> theRunningQueueWorkers = null;
		List<Container<JobContext, QueueWorker>> thePendingQueueWorkers = null;
		synchronized (this) {
			if(stopped.compareAndSet(false, true)) {
				changeState(state.get(), ParallelWorkerStateListener.STATE_STOPPED, Collections.EMPTY_LIST, false);
				theRunningQueueWorkers = runningQueueWorkers;
				thePendingQueueWorkers = pendingQueueWorkers;
			}
		}
		if(theRunningQueueWorkers != null) {
			for(QueueWorker asyncQueueWorker : theRunningQueueWorkers.values()) {
				CommonUtils.ignoreAnyError(asyncQueueWorker::stop, TAG);
			}
		}
		if(thePendingQueueWorkers != null) {
			for(Container<JobContext, QueueWorker> container : thePendingQueueWorkers) {
				CommonUtils.ignoreAnyError(() -> container.getP().stop(), TAG);
			}
		}
	}
	private void startAsyncQueueWorker(JobContext jobContext, QueueWorker asyncQueueWorker) {
		asyncQueueWorker.setQueueWorkerStateListener((id, fromState, toState) -> {
			if(toState == QueueWorkerStateListener.STATE_STOPPED || toState == QueueWorkerStateListener.STATE_FINISHED) {
//				asyncQueueWorker.stop();
				removeRunningToExecutePendingWorker(id);
			}
		});
		asyncQueueWorker.start(jobContext, startOnCurrentThread);
	}

	private synchronized void removeRunningToExecutePendingWorker(String id) {
		QueueWorker worker = runningQueueWorkers.remove(id); //this worker is already in stopped state.
		if(worker != null)
			CommonUtils.ignoreAnyError(worker::stop, TAG);
		if(state.get() == ParallelWorkerStateListener.STATE_RUNNING && worker != null && !pendingQueueWorkers.isEmpty() && !stopped.get()) {
			startImmediately();
		} else if(pendingQueueWorkers.isEmpty() && runningQueueWorkers.isEmpty()) {
			if(finishedRunnable != null) {
				workerFinished();
			} /*else {
				changeState(ParallelWorkerStateListener.STATE_RUNNING, ParallelWorkerStateListener.STATE_IDLE, null, true);
			}*/
		}
	}

	private void workerFinished() {
		if(finished.compareAndSet(false, true)) {
			changeState(ParallelWorkerStateListener.STATE_RUNNING, ParallelWorkerStateListener.STATE_FINISHED, null, false);
			finishedRunnable.run();
		}
	}

	@Override
	public void start() {
		start(false);
	}

	@Override
	public void start(boolean startOnCurrentThread) {
		if(changeState(state.get(), ParallelWorkerStateListener.STATE_RUNNING, list(ParallelWorkerStateListener.STATE_NONE), false)) {
			this.startOnCurrentThread = startOnCurrentThread;
			startImmediately();
		}
	}
	private synchronized void startImmediately() {
		while(!pendingQueueWorkers.isEmpty() && !stopped.get() && runningQueueWorkers.size() < parallelCount) {
			Container<JobContext, QueueWorker> container = pendingQueueWorkers.remove(0);
			if(container != null) {
				if(runningQueueWorkers.size() < parallelCount) {
					QueueWorker asyncQueueWorker = container.getP();
					QueueWorker old = runningQueueWorkers.putIfAbsent(asyncQueueWorker.getId(), asyncQueueWorker);
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
	public Collection<QueueWorker> runningQueueWorkers() {
		return runningQueueWorkers.values();
	}

	@Override
	public List<Container<JobContext, QueueWorker>> pendingQueueWorkers() {
		return pendingQueueWorkers;
	}

	@Override
	public synchronized void finished(Runnable finishedRunnable) {
		this.finishedRunnable = finishedRunnable;

		if(this.finishedRunnable != null && runningQueueWorkers.isEmpty() && pendingQueueWorkers.isEmpty()) {
			workerFinished();
		}
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
