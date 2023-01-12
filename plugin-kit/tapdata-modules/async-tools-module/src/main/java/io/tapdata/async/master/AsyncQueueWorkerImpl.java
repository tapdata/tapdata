package io.tapdata.async.master;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.Container;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.modules.api.async.master.*;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.tapdata.entity.simplify.TapSimplify.list;
import static io.tapdata.async.master.QueueWorkerStateListener.*;

/**
 * @author aplomb
 */
public class AsyncQueueWorkerImpl implements QueueWorker, Runnable {
	private static final String TAG = AsyncQueueWorkerImpl.class.getSimpleName();
	private final String id;
	private final AsyncJobChainImpl asyncJobChain;
	private final ThreadPoolExecutor threadPoolExecutor;
	private JobContext initialJobContext;
	private JobErrorListener asyncJobErrorListener;
	private QueueWorkerStateListener queueWorkerStateListener;
	private final TapUtils tapUtils;
	private final AtomicInteger state = new AtomicInteger(STATE_NONE);

	private JobContext currentJobContext;

	private boolean startOnCurrentThread = false;
//	private ScheduledFuture<?> longIdleScheduleFuture;
	private final ExecutorsManager executorsManager;
//	@Bean
//	private DeadThreadPoolCleaner deadThreadPoolCleaner;
	private final Map<String, Class<? extends Job>> asyncJobMap;
	private Runnable threadBefore;
	private Runnable threadAfter;

	public AsyncQueueWorkerImpl(String id, Map<String, Class<? extends Job>> asyncJobMap) {
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
	public QueueWorker job(JobChain asyncJobChain) {
		if(asyncJobChain != null) {
			Set<Map.Entry<String, JobBase>> asyncJobCollection = asyncJobChain.asyncJobs();
			for(Map.Entry<String, JobBase> entry : asyncJobCollection) {
				JobBase jobBase = entry.getValue();
				if(jobBase instanceof Job) {
					this.asyncJobChain.job(entry.getKey(), (Job) entry.getValue(), asyncJobChain.isPending(entry.getKey()));
				} else if(jobBase instanceof AsyncJob){
					this.asyncJobChain.asyncJob(entry.getKey(), (AsyncJob) entry.getValue(), asyncJobChain.isPending(entry.getKey()));
				} else {
					throw new CoreException(AsyncErrors.UNKNOWN_JOB, "Unknown job {}", jobBase);
				}
			}
			if (state.get() > STATE_NONE && state.get() < STATE_STOPPED)
				startPrivate();
		}
		return this;
	}

	@Override
	public QueueWorker job(Job asyncJob) {
		return job(UUID.randomUUID().toString(), asyncJob);
	}

	@Override
	public QueueWorker job(String id, Job asyncJob) {
		return job(id, asyncJob, false);
	}
	@Override
	public QueueWorker job(String id, Job asyncJob, boolean pending) {
		asyncJobChain.job(id, asyncJob, pending);
		if (!pending && state.get() > STATE_NONE && state.get() < STATE_STOPPED)
			startPrivate();
		return this;
	}

	@Override
	public QueueWorker finished() {
		asyncJobChain.job((LastJob) jobContext -> null);
		return this;
	}
	@Override
	public QueueWorker finished(String id, Job job) {
		asyncJobChain.job(id, (LastJob) job::run);
		return this;
	}
	@Override
	public QueueWorker finished(Job job) {
		asyncJobChain.job((LastJob) job::run);
		return this;
	}

	@Override
	public QueueWorker externalJob(String id, Function<JobContext, JobContext> jobContextConsumer) {
		return externalJob(id, jobContextConsumer, false);
	}

	@Override
	public QueueWorker externalJob(String id, Function<JobContext, JobContext> jobContextConsumer, boolean pending) {
		return externalJob(id, null, jobContextConsumer, pending);
	}
	@Override
	public QueueWorker externalJob(String id, Job asyncJob, Function<JobContext, JobContext> jobContextConsumer) {
		return externalJob(id, asyncJob, jobContextConsumer, false);
	}

	@Override
	public QueueWorker externalJob(String id, Job asyncJob, Function<JobContext, JobContext> jobContextConsumer, boolean pending) {
		asyncJobChain.externalJob(id, asyncJob, jobContextConsumer, pending);
		if (!pending && state.get() > STATE_NONE && state.get() < STATE_STOPPED)
			startPrivate();
		return this;
	}

	@Override
	public QueueWorker asyncJob(AsyncJob asyncJob, boolean pending) {
		asyncJob(UUID.randomUUID().toString(), asyncJob, false);
		return this;
	}
	@Override
	public QueueWorker asyncJob(AsyncJob asyncJob) {
		asyncJob(UUID.randomUUID().toString(), asyncJob, false);
		return this;
	}

	@Override
	public QueueWorker asyncJob(String id, AsyncJob asyncJob) {
		asyncJob(id, asyncJob, false);
		return this;
	}

	@Override
	public QueueWorker asyncJob(String id, AsyncJob asyncJob, boolean pending) {
		asyncJobChain.asyncJob(id, asyncJob, pending);
		if (!pending && state.get() > STATE_NONE && state.get() < STATE_STOPPED)
			startPrivate();
		return this;
	}

	public synchronized QueueWorker cancelAll(String reason) {
		asyncJobChain.asyncJobLinkedMap.clear();
		if(currentJobContext != null) {
			currentJobContext.stop(reason);
		}
		return this;
	}
	@Override
	public synchronized QueueWorker cancelAll() {
		return cancelAll("Canceled");
	}

	@Override
	public synchronized QueueWorker cancel(String id) {
		return cancel(id, false);
	}

	@Override
	public synchronized QueueWorker cancel(String id, boolean immediately) {
		JobBase asyncJob = asyncJobChain.remove(id);
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
	public QueueWorker setAsyncJobErrorListener(JobErrorListener listener) {
		asyncJobErrorListener = listener;
		return this;
	}

	@Override
	public QueueWorker setQueueWorkerStateListener(QueueWorkerStateListener listener) {
		queueWorkerStateListener = listener;
		return this;
	}

	@Override
	public QueueWorker start(JobContext jobContext) {
		start(jobContext, false);
		return this;
	}

	@Override
	public QueueWorker start(JobContext jobContext, boolean startOnCurrentThread) {
		if(changeState(state.get(), STATE_RUNNING, list(STATE_NONE), false)) {
			this.startOnCurrentThread = startOnCurrentThread;
			initialJobContext = jobContext;
			startPrivate();
		} else {
			TapLogger.warn(TAG, "AsyncQueueWorker {} started already, can not start again, state {}", id, state.get());
		}
		return this;
	}

	@Override
	public QueueWorker stop() {
		if(state.get() != STATE_STOPPED) {
			changeState(state.get(), STATE_STOPPED, Collections.EMPTY_LIST, false);
			cancelAll("Stopped");
			threadPoolExecutor.shutdownNow();
		}
		return this;
	}

	@Override
	public QueueWorker threadBefore(Runnable runnable) {
		this.threadBefore = runnable;
		return this;
	}

	@Override
	public QueueWorker threadAfter(Runnable runnable) {
		this.threadAfter = runnable;
		return this;
	}

	private void startPrivate() {
		if (!asyncJobChain.asyncJobLinkedMap.isEmpty()) {
			if (startOnCurrentThread) {
				this.run();
			} else {
				threadPoolExecutor.execute(this);
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
//		if(result) {
//			if(toState != STATE_IDLE) {
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
//					changeState(STATE_IDLE, STATE_LONG_IDLE, null, false);
//				}, 1, TimeUnit.SECONDS);
//			}
//		}
		return result;
	}

	@Override
	public void run() {
		Container<JobBase, JobContext> container = popJob();
		if(container != null) {
			if(threadBefore != null)
				threadBefore.run();
			JobBase asyncJob = container.getT();
			currentJobContext = container.getP();
			if(asyncJob instanceof Job) {
				Job job = (Job) asyncJob;
				boolean realError = false;
				try {
					JobContext theJobContext = job.run(currentJobContext);
					configNextJobContext(theJobContext);
				} catch(Throwable throwable) {
					realError = true;
					if(throwable instanceof CoreException) {
						CoreException coreException = (CoreException) throwable;
						if(coreException.getCode() == AsyncErrors.ASYNC_JOB_STOPPED) {
							realError = false;
						}
					}
					if(realError) {
						TapLogger.error(TAG, "Execute job id {} job {} failed, {}", currentJobContext.getId(), asyncJob, tapUtils.getStackTrace(throwable));
						if(asyncJobErrorListener != null) {
							try {
								asyncJobErrorListener.errorOccurred(currentJobContext.getId(), job, throwable);
							} catch (Throwable throwable1) {
								TapLogger.error(TAG, "Execute job's errorOccurred id {} asyncJob {} failed, {}", currentJobContext.getId(), asyncJob, tapUtils.getStackTrace(throwable1));
							}
							return;
						}
					}
				} finally {
					if(asyncJob instanceof LastJob) {
						changeState(STATE_RUNNING, STATE_FINISHED, null, false);
						threadPoolExecutor.shutdownNow();
					} else {
//						changeState(STATE_RUNNING, STATE_IDLE, null, true);
						if(!realError)
							startPrivate();
					}
				}
			} else if(asyncJob instanceof AsyncJob) {
				AsyncJob theAsyncJob = (AsyncJob) asyncJob;
				AtomicBoolean executed = new AtomicBoolean(false);
				AtomicBoolean realError = new AtomicBoolean(false);
				try {
					theAsyncJob.run(currentJobContext, (jobContext, throwable) -> {
						if(executed.compareAndSet(false, true)) {
							if(throwable != null) {
								realError.set(true);
								if(throwable instanceof CoreException) {
									CoreException coreException = (CoreException) throwable;
									if(coreException.getCode() == AsyncErrors.ASYNC_JOB_STOPPED) {
										realError.set(false);
									}
								}
								if(realError.get()) {
									TapLogger.error(TAG, "Execute job id {} asyncJob {} failed, {}", currentJobContext.getId(), asyncJob, tapUtils.getStackTrace(throwable));
									if(asyncJobErrorListener != null) {
										try {
											asyncJobErrorListener.errorOccurred(currentJobContext.getId(), theAsyncJob, throwable);
										} catch (Throwable throwable1) {
											TapLogger.error(TAG, "Execute asyncJob's errorOccurred id {} asyncJob {} failed, {}", currentJobContext.getId(), asyncJob, tapUtils.getStackTrace(throwable1));
										}
									}
								}
							} else {
								configNextJobContext(jobContext);
							}
	//						changeState(STATE_RUNNING, STATE_IDLE, null, true);
							if(!realError.get())
								startPrivate();
						}
					});
				} catch(Throwable throwable) {
					realError.set(true);
					if(throwable instanceof CoreException) {
						CoreException coreException = (CoreException) throwable;
						if(coreException.getCode() == AsyncErrors.ASYNC_JOB_STOPPED) {
							realError.set(false);
						}
					}
					if(realError.get()) {
						TapLogger.error(TAG, "Execute asyncJob id {} asyncJob {} failed, {}", currentJobContext.getId(), asyncJob, tapUtils.getStackTrace(throwable));
						if(asyncJobErrorListener != null) {
							try {
								asyncJobErrorListener.errorOccurred(currentJobContext.getId(), theAsyncJob, throwable);
							} catch (Throwable throwable1) {
								TapLogger.error(TAG, "Execute asyncJob's errorOccurred id {} asyncJob {} failed, {}", currentJobContext.getId(), asyncJob, tapUtils.getStackTrace(throwable1));
							}
							return;
						}
					}
				} finally {
					if(asyncJob instanceof LastJob) {
						changeState(STATE_RUNNING, STATE_FINISHED, null, false);
						threadPoolExecutor.shutdownNow();
					} /*else {
//						changeState(STATE_RUNNING, STATE_IDLE, null, true);
						if(!realError.get())
							startPrivate();
					}*/
				}
			}
		} /*else {
			changeState(STATE_RUNNING, STATE_IDLE, null, true);
		}*/
	}

	private Container<JobBase, JobContext> popJob() {
		String first;
		String jumpTo = null;
//		JobContext currentJobContext = null;
		if(initialJobContext != null) {
			currentJobContext = initialJobContext;
			initialJobContext = null;
		}
		Map<String, JobBase> asyncJobLinkedMap;
		asyncJobLinkedMap = asyncJobChain.asyncJobLinkedMap;
		synchronized (this) {
			while((first = asyncJobLinkedMap.keySet().stream().findFirst().orElse(null)) != null) {
				JobBase asyncJob;
				asyncJob = asyncJobLinkedMap.remove(first);
				if (asyncJob == null)
					return null;
				if (currentJobContext != null) {
					jumpTo = currentJobContext.getJumpToId();
					if (jumpTo != null)
						asyncJobChain.pendingJobIds.remove(jumpTo);
				}
				if (asyncJobChain.pendingJobIds.contains(first))
					continue;
				if (jumpTo != null && !jumpTo.equals(first))
					continue;
				if (currentJobContext == null) {
					currentJobContext = new JobContextImpl().asyncQueueWorker(this);
				}
				currentJobContext.resetStop();
				currentJobContext.asyncJob(asyncJob);
				currentJobContext.id(first);
//				runningId = first;
				return new Container<>(asyncJob, currentJobContext);
			}
		}
		return null;
	}
	/*public void run1() {
		try {
			if(threadBefore != null)
				threadBefore.run();
			String first;
			JobContext lastJobContext = null;
			String jumpTo = null;
			if(initialJobContext != null) {
				currentJobContext = initialJobContext;
				initialJobContext = null;
			}
			Map<String, JobBase> asyncJobLinkedMap;
			if(periodScheduleFuture != null) {
				asyncJobLinkedMap = asyncJobChain.cloneChain();
			} else {
				asyncJobLinkedMap = asyncJobChain.asyncJobLinkedMap;
			}
			while((first = asyncJobLinkedMap.keySet().stream().findFirst().orElse(null)) != null) {
				JobBase asyncJob;
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
				lastJobContext = currentJobContext;
				if(asyncJob instanceof Job) {
					Job job = (Job) asyncJob;
					try {
						JobContext theJobContext = job.run(currentJobContext);
						configNextJobContext(theJobContext);
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
									asyncJobErrorListener.errorOccurred(first, job, throwable);
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
				} else if(asyncJob instanceof AsyncJob) {
					AsyncJob theAsyncJob = (AsyncJob) asyncJob;
					String finalFirst = first;
					theAsyncJob.run(currentJobContext, (jobContext, throwable) -> {
						if(throwable != null) {
							boolean realError = true;
							if(throwable instanceof CoreException) {
								CoreException coreException = (CoreException) throwable;
								if(coreException.getCode() == AsyncErrors.ASYNC_JOB_STOPPED) {
									realError = false;
								}
							}
							if(realError) {
								TapLogger.error(TAG, "Execute job id {} asyncJob {} failed, {}", finalFirst, asyncJob, tapUtils.getStackTrace(throwable));
								if(asyncJobErrorListener != null) {
									try {
										asyncJobErrorListener.errorOccurred(finalFirst, theAsyncJob, throwable);
									} catch (Throwable throwable1) {
										TapLogger.error(TAG, "Execute job's errorOccurred id {} asyncJob {} failed, {}", finalFirst, asyncJob, tapUtils.getStackTrace(throwable1));
									}
									break;
								} else {
									throw throwable;
								}
							}
						} else {
							configNextJobContext(jobContext);
						}
					});
				}
			}
		} finally {
			changeState(STATE_RUNNING, STATE_IDLE, null, true);
			if(threadAfter != null)
				threadAfter.run();
			startPrivate();
		}
	}*/

	private void configNextJobContext(JobContext theJobContext) {
		if(theJobContext == null) {
			theJobContext = new JobContextImpl().asyncQueueWorker(this).context(currentJobContext.getContext());
		} else  {
			if(theJobContext.getContext() == null || (currentJobContext.getContext() != null && !theJobContext.getContext().equals(currentJobContext.getContext()))) {
				theJobContext.setContext(currentJobContext.getContext());
			}
		}
		currentJobContext = theJobContext;
	}

	public int getState() {
		return state.get();
	}
}
