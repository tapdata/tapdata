package io.tapdata.flow.engine.V2.task.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.task.config.TaskGlobalVariable;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.taskinspect.ITaskInspect;
import com.tapdata.taskinspect.TaskInspectHelper;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.taskinspect.TaskInspectUtils;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.flow.engine.V2.common.HazelcastStatusMappingEnum;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.entity.TaskEnvMap;
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.node.hazelcast.controller.SnapshotOrderService;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.AdjustBatchSizeFactory;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TerminalMode;
import io.tapdata.flow.engine.V2.util.ConsumerImpl;
import io.tapdata.flow.engine.V2.util.SupplierImpl;
import io.tapdata.inspect.AutoRecovery;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.util.TokenBucketRateLimiter;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.task.skiperrortable.ISkipErrorTable;
import io.tapdata.threadgroup.CpuMemoryCollector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jackin
 * @date 2021/12/7 9:47 PM
 **/
public class HazelcastTaskClient implements TaskClient<TaskDto> {

	public static final String TAG = HazelcastTaskClient.class.getSimpleName();
	public static final int MAX_RETRY_TIME = 3;
	public static final long RESET_RETRY_DURATION_HOUR = TimeUnit.HOURS.toMillis(2L);
	public static final int WAIT_JET_JOB_RUNNING_WHEN_STARTING_STATUS_TIME = 5;
	private static final int WAIT_JET_JOB_TERMINAL_STATUS_TIME = 60;
	private static final long WAIT_JET_JOB_TERMINAL_STATUS_INTERVAL_MILLIS = 100L;
	private Logger logger = LogManager.getLogger(HazelcastTaskClient.class);

	private Job job;
	private TaskDto taskDto;
	//  private BaseMetrics taskMetrics;
//  protected ScheduledExecutorService metricsThreadPool;
//  protected ScheduledFuture<?> metricsThreadPoolFuture;
	private ConfigurationCenter configurationCenter;
	private ClientMongoOperator clientMongoOperator;
	private ClientMongoOperator pingClientMongoOperator;
	private HazelcastInstance hazelcastInstance;
	private MonitorManager monitorManager;
	private String cacheName;
	private Throwable error;
	private TerminalMode terminalMode;
	private long lastRetryTimeMillis;
	private final AtomicInteger retryCounter;
	private AtomicBoolean retrying;
	private final ITaskInspect taskInspect;
	private final AutoRecovery autoRecovery;
	private final ISkipErrorTable skipErrorTable;
	private final boolean recoveryClient;
    private final long createTime = System.currentTimeMillis();

	public static HazelcastTaskClient create(TaskDto taskDto, ClientMongoOperator clientMongoOperator, ClientMongoOperator pingClientMongoOperator,
											 ConfigurationCenter configurationCenter, HazelcastInstance hazelcastInstance) {
		return new HazelcastTaskClient(null, taskDto, clientMongoOperator, pingClientMongoOperator, configurationCenter, hazelcastInstance, false);
	}

	/**
	 * Creates a client for a temporary DLQ replay job.  It deliberately does
	 * not register the formal task's ping monitor, retry state, or task-inspect
	 * state.  The temporary job has its own task id and owns only its own
	 * aspect session; it must not own the formal task lifecycle.
	 */
	public static HazelcastTaskClient createDqlRecovery(TaskDto taskDto,
											 ClientMongoOperator clientMongoOperator,
											 ClientMongoOperator pingClientMongoOperator,
											 ConfigurationCenter configurationCenter,
											 HazelcastInstance hazelcastInstance) {
		return new HazelcastTaskClient(null, taskDto, clientMongoOperator, pingClientMongoOperator,
				configurationCenter, hazelcastInstance, true);
	}

	public HazelcastTaskClient(Job job, TaskDto taskDto, ClientMongoOperator clientMongoOperator, ConfigurationCenter configurationCenter, HazelcastInstance hazelcastInstance) {
		this(job, taskDto, clientMongoOperator, clientMongoOperator, configurationCenter, hazelcastInstance);
	}

	public HazelcastTaskClient(Job job, TaskDto taskDto, ClientMongoOperator clientMongoOperator, ClientMongoOperator pingClientMongoOperator,
								   ConfigurationCenter configurationCenter, HazelcastInstance hazelcastInstance) {
		this(job, taskDto, clientMongoOperator, pingClientMongoOperator, configurationCenter, hazelcastInstance, false);
	}

	private HazelcastTaskClient(Job job, TaskDto taskDto, ClientMongoOperator clientMongoOperator,
								   ClientMongoOperator pingClientMongoOperator,
								   ConfigurationCenter configurationCenter,
								   HazelcastInstance hazelcastInstance,
								   boolean recoveryClient) {
		this.job = job;
		this.taskDto = taskDto;
		this.clientMongoOperator = clientMongoOperator;
		this.pingClientMongoOperator = pingClientMongoOperator;
		this.configurationCenter = configurationCenter;
		this.hazelcastInstance = hazelcastInstance;
		this.recoveryClient = recoveryClient;
		if (!recoveryClient && !taskDto.isTestTask() && !taskDto.isPreviewTask()) {
			this.monitorManager = new MonitorManager();
			try {
				this.monitorManager.startMonitor(MonitorManager.MonitorType.TASK_PING_TIME, taskDto, pingClientMongoOperator, new SupplierImpl<>(this::stop), new ConsumerImpl<>(this::terminalMode));
			} catch (Exception e) {
				logger.warn("The task ping time monitor failed to start, which may affect the ping time functionality; Error: "
						+ e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			}
			this.autoRecovery = AutoRecovery.init(taskDto.getId().toHexString());
		} else {
			this.autoRecovery = null;
        }
		this.skipErrorTable = recoveryClient ? null : ISkipErrorTable.create(taskDto, clientMongoOperator);
		Optional<Node> cacheNode = taskDto.getDag().getNodes().stream().filter(n -> n instanceof CacheNode).findFirst();
		cacheNode.ifPresent(c -> cacheName = ((CacheNode) c).getCacheName());
		this.retryCounter = new AtomicInteger(0);
		this.retrying = new AtomicBoolean(false);
		this.taskInspect = recoveryClient ? null : TaskInspectHelper.create(taskDto, clientMongoOperator);
	}

	@Override
	public String getStatus() {
		try {
			return HazelcastStatusMappingEnum.fromJobStatus(job.getStatus());
		} catch (com.hazelcast.jet.core.JobNotFoundException e) {
			logger.warn("Job with id {} not found in Hazelcast cluster when getting status. Task: {}[{}]",
					job.getId(), taskDto.getName(), taskDto.getId().toHexString());
			return HazelcastStatusMappingEnum.fromJobStatus(JobStatus.FAILED);
		}
	}

	public JobStatus getJetStatus() {
		try {
			return job.getStatus();
		} catch (com.hazelcast.jet.core.JobNotFoundException e) {
			logger.warn("Job with id {} not found in Hazelcast cluster when getting jet status. Task: {}[{}]",
					job.getId(), taskDto.getName(), taskDto.getId().toHexString());
			return JobStatus.FAILED;
		}
	}

	@Override
	public TaskDto getTask() {
		return taskDto;
	}

	@Override
	public String getCacheName() {
		return cacheName;
	}

	@Override
	public synchronized void terminalMode(TerminalMode terminalMode) {
		switch (terminalMode) {
			case STOP_GRACEFUL:
				this.terminalMode = terminalMode;
				break;
			case INTERNAL_STOP:
				this.terminalMode = terminalMode;
				break;
			case ERROR:
				if (TerminalMode.STOP_GRACEFUL != this.terminalMode) {
					this.terminalMode = terminalMode;
				}
				break;
			case COMPLETE:
				if (TerminalMode.STOP_GRACEFUL != this.terminalMode
						&& TerminalMode.ERROR != this.terminalMode) {
					this.terminalMode = terminalMode;
				}
				break;
		}
	}

	@Override
	public TerminalMode getTerminalMode() {
		return terminalMode;
	}

	@Override
	public synchronized boolean stop() {
		if(null == job) {
			return false;
		}
		try {
			JobStatus status = getJetStatus();
			for (int i = 0; i < WAIT_JET_JOB_RUNNING_WHEN_STARTING_STATUS_TIME; i++) {
				if (status == JobStatus.STARTING) {
					try {
						TimeUnit.SECONDS.sleep(1L);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;
					}
					status = getJetStatus();
				} else {
					break;
				}
			}

			// Jet suspend is asynchronous.  In particular, after suspend() returns
			// the public status can remain RUNNING while Jet is internally in
			// SUSPEND_FORCEFUL.  Issuing cancel() during that window fails with
			// "Job is already terminating".  Only request cancellation once Jet has
			// reached SUSPENDED (or when a STARTING job can be cancelled directly),
			// then let waitForTerminalStatus observe the remaining transitions.
			boolean cancelRequested = false;
			if (status == JobStatus.RUNNING) {
				try {
					job.suspend();
				} catch (IllegalStateException exception) {
					if (!isJetTerminationInProgress(exception)) {
						throw exception;
					}
					// A previous stop request may have already submitted suspend.  Keep
					// observing that request instead of treating it as a new failure.
					cancelRequested = isJetCancelInProgress(exception);
					logger.info("Jet job {} is already transitioning to stop; continue waiting. reason={}",
							job.getId(), exception.getMessage());
				}
			} else if (status == JobStatus.SUSPENDED || status == JobStatus.STARTING) {
				job.cancel();
				cancelRequested = true;
			}
			return waitForTerminalStatus(cancelRequested);
		} catch (com.hazelcast.jet.core.JobNotFoundException e) {
			logger.warn("Job with id {} not found in Hazelcast cluster when stopping. Task: {}[{}]. Considering task as stopped.",
					job.getId(), taskDto.getName(), taskDto.getId().toHexString());
			close();
			return true;
		}
	}

	private boolean waitForTerminalStatus(boolean cancelRequested) {
		long deadline = System.currentTimeMillis()
				+ TimeUnit.SECONDS.toMillis(WAIT_JET_JOB_TERMINAL_STATUS_TIME);
		try {
			while (System.currentTimeMillis() < deadline) {
				JobStatus status = getJetStatus();
				if (status.isTerminal()) {
					close();
					return true;
				}
				// Do not cancel while a previously requested suspend is still
				// transitioning.  SUSPENDED is the first public state in which
				// Jet accepts the follow-up cancel command.
				if (!cancelRequested && status == JobStatus.SUSPENDED) {
					try {
						job.cancel();
						cancelRequested = true;
					} catch (IllegalStateException exception) {
						if (!isJetTerminationInProgress(exception)) {
							throw exception;
						}
						// Another cancellation won the race.  It is already sufficient;
						// wait for Jet to report the terminal state.
						cancelRequested = true;
					}
				}
				TimeUnit.MILLISECONDS.sleep(WAIT_JET_JOB_TERMINAL_STATUS_INTERVAL_MILLIS);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return false;
	}

	private boolean isJetTerminationInProgress(IllegalStateException exception) {
		return exception.getMessage() != null && exception.getMessage().contains("already terminating");
	}

	private boolean isJetCancelInProgress(IllegalStateException exception) {
		return exception.getMessage() != null && exception.getMessage().contains("CANCEL");
	}

	@Override
	public void close() {
		if (recoveryClient) {
			CommonUtils.ignoreAnyError(
					() -> AspectUtils.executeAspect(new TaskStopAspect().task(taskDto).error(error)), TAG);
			cleanupRecoveryTaskState();
			return;
		}
		CommonUtils.handleAnyError(
				() -> AdjustBatchSizeFactory.unregister(taskDto.getId().toHexString()),
				err -> logger.warn("Unregister 'Adjust batch size' task failed, error: {}", err.getMessage())
		);
		ObsLogger obsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskDto);
		CommonUtils.handleAnyError(
				() -> {
					if (monitorManager != null) monitorManager.close();
					obsLogger.trace(String.format("Closed task monitor(s)\n%s", monitorManager));
				},
				err -> {
					obsLogger.warn(String.format("Close task monitor(s) failed, error: %s\n  %s", err.getMessage(), Log4jUtil.getStackString(err)));
				}
		);
        CommonUtils.handleAnyError(
            () -> {
                if (null != taskInspect) taskInspect.close();
                obsLogger.trace("Closed {} instance\n  {}", TaskInspectUtils.MODULE_NAME, taskInspect);
            },
            err -> {
                obsLogger.warn("Closed {} instance failed, error: {}\n  {}", TaskInspectUtils.MODULE_NAME, err.getMessage(), Log4jUtil.getStackString(err));
            }
        );
            CommonUtils.handleAnyError(
                () -> {
					if(null != autoRecovery) autoRecovery.close();
                    obsLogger.trace(String.format("Closed task auto recovery instance\n  %s", autoRecovery));
                },
                err -> {
                    obsLogger.warn(String.format("Closed task auto recovery instance failed, error: %s\n  %s", err.getMessage(), Log4jUtil.getStackString(err)));
                }
            );
            CommonUtils.handleAnyError(
                () -> {
					skipErrorTable.close();
                    obsLogger.trace(String.format("Closed task skip error table instance %s", skipErrorTable));
                },
                err -> {
                    obsLogger.warn(String.format("Closed task skip error table failed, error: %s\n  %s", err.getMessage(), Log4jUtil.getStackString(err)));
                }
            );
		CommonUtils.handleAnyError(
				() -> {
					AspectUtils.executeAspect(new TaskStopAspect().task(taskDto).error(error));
					obsLogger.trace("Stopped task aspect(s)");
				},
				err -> {
					obsLogger.warn(String.format("Stop task aspect(s) failed, error: %s\n  %s", err.getMessage(), Log4jUtil.getStackString(err)));
				}
		);
		CommonUtils.handleAnyError(
				() -> {
					if (SnapshotOrderService.getInstance().removeController(taskDto.getId().toHexString())) {
						obsLogger.trace("Snapshot order controller have been removed");
					}
				},
				error -> obsLogger.warn("Remove snapshot order controller failed, error: %s\n %s", error.getMessage(), Log4jUtil.getStackString(error))
		);
		CommonUtils.ignoreAnyError(() -> TaskGlobalVariable.INSTANCE.removeTask(taskDto.getId().toHexString()), TAG);
		CommonUtils.ignoreAnyError(() -> TokenBucketRateLimiter.get().remove(taskDto.getId().toHexString()), TAG);
	}

	private void cleanupRecoveryTaskState() {
		String taskId = taskDto == null || taskDto.getId() == null ? null : taskDto.getId().toHexString();
		if (taskId == null) {
			return;
		}
		CommonUtils.ignoreAnyError(() -> CpuMemoryCollector.unregisterTask(taskId), TAG);
		CommonUtils.ignoreAnyError(() -> TaskGlobalVariable.INSTANCE.removeTask(taskId), TAG);
		CommonUtils.ignoreAnyError(() -> PdkStateMap.globalStateMap(hazelcastInstance).remove(TaskEnvMap.name(taskId)), TAG);
		CommonUtils.ignoreAnyError(() -> TokenBucketRateLimiter.get().remove(taskId), TAG);
	}

	@Override
	public void join() {
		this.job.join();
	}

	@Override
	public synchronized void error(Throwable throwable) {
		if (null == error) {
			this.error = throwable;
		}
	}

	@Override
	public synchronized Throwable getError() {
		if (error == null && recoveryClient && job != null) {
			try {
				if (job.getFuture().isCompletedExceptionally()) {
					job.getFuture().join();
				}
			} catch (CompletionException | java.util.concurrent.CancellationException exception) {
				error = unwrapJobFailure(exception);
			} catch (RuntimeException exception) {
				logger.debug("Failed to read DLQ recovery job failure, jobId={}: {}",
						job.getId(), exception.getMessage());
			}
		}
		return error;
	}

	public MonitorManager getTaskMonitorManager() {
		return monitorManager;
	}

	@Override
	public boolean isRunning() {
		try {
			JobStatus status = job.getStatus();
			boolean b = status == JobStatus.STARTING || status == JobStatus.RUNNING;
			if (!b) {
				logger.warn("The task is not running, status:  {} {}", status, Arrays.asList(Thread.currentThread().getStackTrace()));
			}
			return b;
		} catch (com.hazelcast.jet.core.JobNotFoundException e) {
			logger.warn("Job with id {} not found in Hazelcast cluster, task is considered not running. Task: {}[{}]",
					job.getId(), taskDto.getName(), taskDto.getId().toHexString());
			return false;
		}
	}

	@Override
	public boolean canRetry() {
		if (this.retrying.get()) {
			return true;
		}
		if (retryCounter.incrementAndGet() <= MAX_RETRY_TIME) {
			this.lastRetryTimeMillis = System.currentTimeMillis();
			this.retrying.set(true);
			return true;
		}
		long currentTimeMillis = System.currentTimeMillis();
		long retryDuration = currentTimeMillis - lastRetryTimeMillis;
		if (retryDuration >= RESET_RETRY_DURATION_HOUR) {
			this.lastRetryTimeMillis = System.currentTimeMillis();
			this.retryCounter.set(0);
			this.retrying.set(true);
			return true;
		}
		return false;
	}

	@Override
	public boolean resume() {
		JobStatus jobStatus = job.getStatus();
		if (JobStatus.SUSPENDED == jobStatus) {
			job.resume();
			this.retrying.set(false);
			return true;
		}
		return false;
	}

	@Override
	public int getRetryTime() {
		return retryCounter.get();
	}

    @Override
    public long getCreateTime() {
        return createTime;
    }

    public void setJob(Job job) {
		this.job = job;
		if (recoveryClient && job != null) {
			try {
				job.getFuture().whenComplete((ignored, failure) -> {
					if (failure != null) {
						error(unwrapJobFailure(failure));
					}
				});
			} catch (RuntimeException exception) {
				logger.warn("Failed to observe DLQ recovery job failure, jobId={}: {}",
						job.getId(), exception.getMessage());
			}
		}
	}

	private Throwable unwrapJobFailure(Throwable failure) {
		Throwable current = failure;
		while ((current instanceof CompletionException || current instanceof ExecutionException)
				&& current.getCause() != null) {
			current = current.getCause();
		}
		return current;
	}
}
