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
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.node.hazelcast.controller.SnapshotOrderService;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TerminalMode;
import io.tapdata.flow.engine.V2.util.ConsumerImpl;
import io.tapdata.flow.engine.V2.util.SupplierImpl;
import io.tapdata.inspect.AutoRecovery;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.util.TokenBucketRateLimiter;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Optional;
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
	private Logger logger = LogManager.getLogger(HazelcastTaskClient.class);

	private Job job;
	private TaskDto taskDto;
	//  private BaseMetrics taskMetrics;
//  protected ScheduledExecutorService metricsThreadPool;
//  protected ScheduledFuture<?> metricsThreadPoolFuture;
	private ConfigurationCenter configurationCenter;
	private ClientMongoOperator clientMongoOperator;
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
    private final long createTime = System.currentTimeMillis();

	public static HazelcastTaskClient create(TaskDto taskDto, ClientMongoOperator clientMongoOperator, ConfigurationCenter configurationCenter, HazelcastInstance hazelcastInstance) {
		return new HazelcastTaskClient(null, taskDto, clientMongoOperator, configurationCenter, hazelcastInstance);
	}

	public HazelcastTaskClient(Job job, TaskDto taskDto, ClientMongoOperator clientMongoOperator, ConfigurationCenter configurationCenter, HazelcastInstance hazelcastInstance) {
		this.job = job;
		this.taskDto = taskDto;
		this.clientMongoOperator = clientMongoOperator;
		this.configurationCenter = configurationCenter;
		this.hazelcastInstance = hazelcastInstance;
		if (!taskDto.isTestTask() && !taskDto.isPreviewTask()) {
			this.monitorManager = new MonitorManager();
			try {
				this.monitorManager.startMonitor(MonitorManager.MonitorType.TASK_PING_TIME, taskDto, clientMongoOperator, new SupplierImpl<>(this::stop), new ConsumerImpl<>(this::terminalMode));
			} catch (Exception e) {
				logger.warn("The task ping time monitor failed to start, which may affect the ping time functionality; Error: "
						+ e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			}
			this.autoRecovery = AutoRecovery.init(taskDto.getId().toHexString());
		} else {
			this.autoRecovery = null;
        }
		Optional<Node> cacheNode = taskDto.getDag().getNodes().stream().filter(n -> n instanceof CacheNode).findFirst();
		cacheNode.ifPresent(c -> cacheName = ((CacheNode) c).getCacheName());
		this.retryCounter = new AtomicInteger(0);
		this.retrying = new AtomicBoolean(false);
        this.taskInspect = TaskInspectHelper.create(taskDto, clientMongoOperator);
	}

	@Override
	public String getStatus() {
		try {
			return HazelcastStatusMappingEnum.fromJobStatus(job.getStatus());
		} catch (com.hazelcast.jet.core.JobNotFoundException e) {
			logger.warn("Job with id {} not found in Hazelcast cluster when getting status. Task: {}[{}]",
					job != null ? job.getId() : "null", taskDto.getName(), taskDto.getId().toHexString());
			return HazelcastStatusMappingEnum.fromJobStatus(JobStatus.FAILED);
		} catch (Exception e) {
			logger.error("Error getting job status for task {}[{}]: {}",
					taskDto.getName(), taskDto.getId().toHexString(), e.getMessage(), e);
			return HazelcastStatusMappingEnum.fromJobStatus(JobStatus.FAILED);
		}
	}

	public JobStatus getJetStatus() {
		try {
			return job.getStatus();
		} catch (com.hazelcast.jet.core.JobNotFoundException e) {
			logger.warn("Job with id {} not found in Hazelcast cluster when getting jet status. Task: {}[{}]",
					job != null ? job.getId() : "null", taskDto.getName(), taskDto.getId().toHexString());
			return JobStatus.FAILED;
		} catch (Exception e) {
			logger.error("Error getting jet job status for task {}[{}]: {}",
					taskDto.getName(), taskDto.getId().toHexString(), e.getMessage(), e);
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
			for (int i = 0; i < WAIT_JET_JOB_RUNNING_WHEN_STARTING_STATUS_TIME; i++) {
				if (getJetStatus() == JobStatus.STARTING) {
					try {
						TimeUnit.SECONDS.sleep(1L);
					} catch (InterruptedException e) {
						break;
					}
				} else {
					break;
				}
			}
			if (job.getStatus() == JobStatus.RUNNING) {
				job.suspend();
			}
			if (job.getStatus() == JobStatus.SUSPENDED) {
				job.cancel();
			}

			if (job.getStatus().isTerminal()) {
				close();
				return true;
			}
			return false;
		} catch (com.hazelcast.jet.core.JobNotFoundException e) {
			logger.warn("Job with id {} not found in Hazelcast cluster when stopping. Task: {}[{}]. Considering task as stopped.",
					job.getId(), taskDto.getName(), taskDto.getId().toHexString());
			close();
			return true;
		} catch (Exception e) {
			logger.error("Error stopping job for task {}[{}]: {}",
					taskDto.getName(), taskDto.getId().toHexString(), e.getMessage(), e);
			close();
			return true;
		}
	}

	@Override
	public void close() {
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
	public Throwable getError() {
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
					job != null ? job.getId() : "null", taskDto.getName(), taskDto.getId().toHexString());
			return false;
		} catch (Exception e) {
			logger.error("Error checking job status for task {}[{}]: {}",
					taskDto.getName(), taskDto.getId().toHexString(), e.getMessage(), e);
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
	}
}
