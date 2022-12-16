package io.tapdata.async.master;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.modules.api.async.master.*;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.tapdata.entity.simplify.TapSimplify.list;
import static io.tapdata.async.master.QueueWorkerStateListener.*;

/**
 * @author aplomb
 */
public class AsyncQueueWorkerImpl implements AsyncQueueWorker, Runnable {
	private static final String TAG = AsyncQueueWorkerImpl.class.getSimpleName();
	private final String id;
	private final AsyncJobChainImpl asyncJobChain;
	private final ThreadPoolExecutor threadPoolExecutor;
	private JobContext initialJobContext;
	private AsyncJobErrorListener asyncJobErrorListener;
	private QueueWorkerStateListener queueWorkerStateListener;
	private final TapUtils tapUtils;
	private final AtomicInteger state = new AtomicInteger(STATE_NONE);
	private String runningId;

	private JobContext currentJobContext;

	private boolean startOnCurrentThread = false;
	private ScheduledFuture<?> longIdleScheduleFuture;
	private Long delayMilliSeconds;
	private Long periodMilliSeconds;
	private final ExecutorsManager executorsManager;
	private ScheduledFuture<?> periodScheduleFuture;
//	@Bean
//	private DeadThreadPoolCleaner deadThreadPoolCleaner;
	private final Map<String, Class<? extends AsyncJob>> asyncJobMap;

	public AsyncQueueWorkerImpl(String id, Map<String, Class<? extends AsyncJob>> asyncJobMap) {
		this.id = id;
		this.asyncJobMap = asyncJobMap;
		asyncJobChain = new AsyncJobChainImpl(this.asyncJobMap);
		tapUtils = InstanceFactory.instance(TapUtils.class);
		if(tapUtils == null)
			throw new CoreException(AsyncErrors.MISSING_TAP_UTILS, "Missing TapUtils' implementation");
		threadPoolExecutor = AsyncUtils.createThreadPoolExecutor(id, 1, TAG);
		executorsManager = ExecutorsManager.getInstance();
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public AsyncQueueWorker job(AsyncJobChain asyncJobChain) {
		if(asyncJobChain != null) {
			Set<Map.Entry<String, AsyncJob>> asyncJobCollection = asyncJobChain.asyncJobs();
			for(Map.Entry<String, AsyncJob> entry : asyncJobCollection) {
				this.asyncJobChain.job(entry.getKey(), entry.getValue(), asyncJobChain.isPending(entry.getKey()));
			}
			if (state.get() > STATE_NONE && state.get() < STATE_STOPPED)
				startPrivate();
		}
		return this;
	}

	@Override
	public AsyncQueueWorker job(AsyncJob asyncJob) {
		return job(UUID.randomUUID().toString(), asyncJob);
	}

	@Override
	public AsyncQueueWorker job(String id, AsyncJob asyncJob) {
		return job(id, asyncJob, false);
	}
	@Override
	public AsyncQueueWorker job(String id, AsyncJob asyncJob, boolean pending) {
		asyncJobChain.job(id, asyncJob, pending);
		if (!pending && state.get() > STATE_NONE && state.get() < STATE_STOPPED)
			startPrivate();
		return this;
	}

	@Override
	public AsyncQueueWorker externalJob(String id, Function<JobContext, JobContext> jobContextConsumer) {
		return externalJob(id, jobContextConsumer, false);
	}

	@Override
	public AsyncQueueWorker externalJob(String id, Function<JobContext, JobContext> jobContextConsumer, boolean pending) {
		return externalJob(id, null, jobContextConsumer, pending);
	}
	@Override
	public AsyncQueueWorker externalJob(String id, AsyncJob asyncJob, Function<JobContext, JobContext> jobContextConsumer) {
		return externalJob(id, asyncJob, jobContextConsumer, false);
	}

	@Override
	public AsyncQueueWorker externalJob(String id, AsyncJob asyncJob, Function<JobContext, JobContext> jobContextConsumer, boolean pending) {
		asyncJobChain.externalJob(id, asyncJob, jobContextConsumer, pending);
		if (!pending && state.get() > STATE_NONE && state.get() < STATE_STOPPED)
			startPrivate();
		return this;
	}

	public synchronized AsyncQueueWorker cancelAll(String reason) {
		asyncJobChain.asyncJobLinkedMap.clear();
		if(currentJobContext != null) {
			currentJobContext.stop(reason);
		}
		return this;
	}
	@Override
	public synchronized AsyncQueueWorker cancelAll() {
		return cancelAll("Canceled");
	}

	@Override
	public synchronized AsyncQueueWorker cancel(String id) {
		return cancel(id, false);
	}

	@Override
	public synchronized AsyncQueueWorker cancel(String id, boolean immediately) {
		AsyncJob asyncJob = asyncJobChain.remove(id);
		if(asyncJob == null) {
			String currentId = currentJobContext.getId();
			if(currentId != null && currentId.equals(id)) {
				currentJobContext.stop("Canceled");
				if(immediately) {
					//TODO huge risk to isolate dead thread pool into other place, as dead thread may still run concurrently with health thread.
//					deadThreadPoolCleaner.add(id, threadPoolExecutor);
//					threadPoolExecutor = createThreadPoolExecutor(id);
//					startPrivate();
				}
			}
		}
		return this;
	}

	@Override
	public String runningJobId() {
		return runningId;
	}

	@Override
	public AsyncQueueWorker setAsyncJobErrorListener(AsyncJobErrorListener listener) {
		asyncJobErrorListener = listener;
		return this;
	}

	@Override
	public AsyncQueueWorker setQueueWorkerStateListener(QueueWorkerStateListener listener) {
		queueWorkerStateListener = listener;
		return this;
	}

	@Override
	public AsyncQueueWorker start(JobContext jobContext) {
		start(jobContext, false);
		return this;
	}

	@Override
	public AsyncQueueWorker start(JobContext jobContext, boolean startOnCurrentThread) {
		if(changeState(STATE_NONE, STATE_IDLE, null, false)) {
			this.startOnCurrentThread = startOnCurrentThread;
			initialJobContext = jobContext;
			startPrivate();
		} else {
			TapLogger.warn(TAG, "AsyncQueueWorker {} started already, can not start again, state {}", id, state.get());
		}
		return this;
	}

	@Override
	public AsyncQueueWorker start(JobContext jobContext, long delayMilliSeconds, long periodMilliSeconds) {
		if(delayMilliSeconds < 0 || periodMilliSeconds < 50)
			throw new CoreException(AsyncErrors.ILLEGAL_ARGUMENTS, "Illegal arguments for delayMilliSeconds {} periodMilliSeconds {} to start AsyncQueueWorker {}", delayMilliSeconds, periodMilliSeconds, id);
		if(changeState(STATE_NONE, STATE_IDLE, null, false)) {
			initialJobContext = jobContext;
			this.delayMilliSeconds = delayMilliSeconds;
			this.periodMilliSeconds = periodMilliSeconds;
			startPrivate();
		} else {
			TapLogger.warn(TAG, "AsyncQueueWorker {} started already, can not start again, state {}", id, state.get());
		}
		return this;
	}

	@Override
	public AsyncQueueWorker stop() {
		if(state.get() != STATE_STOPPED) {
			changeState(state.get(), STATE_STOPPED, Collections.EMPTY_LIST, false);
			cancelAll("Stopped");
			if(periodScheduleFuture != null) {
				periodScheduleFuture.cancel(true);
				periodScheduleFuture = null;
			}
			threadPoolExecutor.shutdownNow();
		}
		return this;
	}

	private void startPrivate() {
		if (!asyncJobChain.asyncJobLinkedMap.isEmpty() && changeState(state.get(), STATE_RUNNING, list(STATE_IDLE, STATE_LONG_IDLE), false)) {
			if(startOnCurrentThread) {
				this.run();
			} else {
				if(delayMilliSeconds != null && periodMilliSeconds != null && periodScheduleFuture == null) {
					periodScheduleFuture = executorsManager.getScheduledExecutorService().scheduleWithFixedDelay(this::run, delayMilliSeconds, periodMilliSeconds, TimeUnit.MILLISECONDS);
				}
				if(periodScheduleFuture == null) {
					threadPoolExecutor.execute(this);
				}
			}
		}
	}

	private synchronized boolean changeState(int fromState, int toState, List<Integer> ignoreFromComparePossibleStates, boolean scheduleForLongIdle) {
		boolean result = false;
		if(ignoreFromComparePossibleStates == null) {
			result = state.compareAndSet(fromState, toState);
			if(result && queueWorkerStateListener != null) {
				CommonUtils.ignoreAnyError(() -> queueWorkerStateListener.stateChanged(id, fromState, toState), TAG);
			}
		} else {
			if(ignoreFromComparePossibleStates.isEmpty() || ignoreFromComparePossibleStates.contains(state.get())) {
				result = true;
				state.set(toState);
				if(queueWorkerStateListener != null) {
					CommonUtils.ignoreAnyError(() -> queueWorkerStateListener.stateChanged(id, fromState, toState), TAG);
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
	public void run() {
		try {
			String first;
			JobContext lastJobContext = null;
			String jumpTo = null;
			if(initialJobContext != null) {
				currentJobContext = initialJobContext;
				initialJobContext = null;
			}
			Map<String, AsyncJob> asyncJobLinkedMap;
			if(periodScheduleFuture != null) {
				asyncJobLinkedMap = asyncJobChain.cloneChain();
			} else {
				asyncJobLinkedMap = asyncJobChain.asyncJobLinkedMap;
			}
			while((first = asyncJobLinkedMap.keySet().stream().findFirst().orElse(null)) != null) {
				AsyncJob asyncJob;
				synchronized (this) {
					asyncJob = asyncJobLinkedMap.remove(first);
					if(asyncJob == null)
						continue;
					if(currentJobContext != null) {
						jumpTo = currentJobContext.getJumpToId();
						if(jumpTo != null)
							asyncJobChain.pendingJobIds.remove(jumpTo);
					}
					if(asyncJobChain.pendingJobIds.contains(first)) {
						continue;
					}
					if(currentJobContext == null) {
						currentJobContext = new JobContextImpl().asyncQueueWorker(this);
					}
					currentJobContext.resetStop();
					currentJobContext.asyncJob(asyncJob);
					currentJobContext.id(first);
					currentJobContext.alive();
					runningId = first;
				}
				if(jumpTo != null && !jumpTo.equals(first))
					continue;
				try {
					lastJobContext = currentJobContext;
					JobContext theJobContext = asyncJob.run(currentJobContext);
					if(theJobContext == null) {
						theJobContext = new JobContextImpl().asyncQueueWorker(this).context(currentJobContext.getContext());
					} else  {
						if(theJobContext.getContext() == null || (currentJobContext.getContext() != null && !theJobContext.getContext().equals(currentJobContext.getContext()))) {
							theJobContext.setContext(currentJobContext.getContext());
						}
					}
					currentJobContext = theJobContext;
				} catch(Throwable throwable) {
					boolean realError = true;
					if(throwable instanceof CoreException) {
						CoreException coreException = (CoreException) throwable;
						if(coreException.getCode() == AsyncErrors.ASYNC_JOB_STOPPED) {
							realError = false;
						}
					}
					if(realError) {
						TapLogger.error(TAG, "Execute job id {} asyncJob {} failed, {}", first, asyncJob, tapUtils.getStackTrace(throwable));
						if(asyncJobErrorListener != null) {
							try {
								asyncJobErrorListener.errorOccurred(first, asyncJob, throwable);
							} catch (Throwable throwable1) {
								TapLogger.error(TAG, "Execute job's errorOccurred id {} asyncJob {} failed, {}", first, asyncJob, tapUtils.getStackTrace(throwable1));
							}
							break;
						} else {
							throw throwable;
						}
					}
				} finally {
					lastJobContext.resetAlive();
					lastJobContext.stop("Completed");
					runningId = null;
				}
			}
		} finally {
			changeState(STATE_RUNNING, STATE_IDLE, null, true);
			startPrivate();
		}
	}

	public int getState() {
		return state.get();
	}
}
