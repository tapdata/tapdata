package io.tapdata.flow.engine.V2.task.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.flow.engine.V2.common.HazelcastStatusMappingEnum;
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TerminalMode;
import io.tapdata.flow.engine.V2.util.SupplierImpl;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
	private SnapshotProgressManager snapshotProgressManager;
	private String cacheName;
	private Throwable error;
	private TerminalMode terminalMode;
	private long lastRetryTimeMillis;
	private final AtomicInteger retryCounter;
	private AtomicBoolean retrying;

	public HazelcastTaskClient(Job job, TaskDto taskDto, ClientMongoOperator clientMongoOperator, ConfigurationCenter configurationCenter, HazelcastInstance hazelcastInstance) {
		this.job = job;
		this.taskDto = taskDto;
		this.clientMongoOperator = clientMongoOperator;
		this.configurationCenter = configurationCenter;
		this.hazelcastInstance = hazelcastInstance;
		if (!StringUtils.equalsAnyIgnoreCase(taskDto.getSyncType(), TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
			this.monitorManager = new MonitorManager();
			try {
				this.monitorManager.startMonitor(MonitorManager.MonitorType.TASK_PING_TIME, taskDto, clientMongoOperator, new SupplierImpl<>(this::stop));
			} catch (Exception e) {
				logger.warn("The task ping time monitor failed to start, which may affect the ping time functionality; Error: "
						+ e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			}
		}
		Optional<Node> cacheNode = taskDto.getDag().getNodes().stream().filter(n -> n instanceof CacheNode).findFirst();
		cacheNode.ifPresent(c -> cacheName = ((CacheNode) c).getCacheName());
		this.retryCounter = new AtomicInteger(0);
		this.retrying = new AtomicBoolean(false);
	}

	@Override
	public String getStatus() {
		return HazelcastStatusMappingEnum.fromJobStatus(job.getStatus());
	}

	public JobStatus getJetStatus() {
		return job.getStatus();
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
		if (job.getStatus() == JobStatus.RUNNING) {
			job.suspend();
		}
		if (job.getStatus() == JobStatus.SUSPENDED) {
			job.cancel();
		}

		if (job.getStatus() == JobStatus.SUSPENDED || job.getStatus() == JobStatus.FAILED || job.getStatus() == JobStatus.COMPLETED) {
			ObsLogger obsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskDto);
			CommonUtils.handleAnyError(
					() -> {
						monitorManager.close();
						logger.info("Closed task monitor(s)\n{}", monitorManager);
						obsLogger.info(String.format("Closed task monitor(s)\n%s", monitorManager));
					},
					err -> {
						logger.warn("Close task monitor(s) failed, error: {}", err.getMessage(), err);
						obsLogger.warn(String.format("Close task monitor(s) failed, error: %s\n  %s", err.getMessage(), Log4jUtil.getStackString(err)));
					}
			);
			CommonUtils.handleAnyError(
					() -> {
						AspectUtils.executeAspect(new TaskStopAspect().task(taskDto).error(error));
						logger.info("Stopped task aspect(s)");
						obsLogger.info("Stopped task aspect(s)");
					},
					err -> {
						logger.warn("Stop task aspect(s) failed, error: {}", err.getMessage(), err);
						obsLogger.warn(String.format("Stop task aspect(s) failed, error: %s\n  %s", err.getMessage(), Log4jUtil.getStackString(err)));
					}
			);
		}
		return job.getStatus().isTerminal();
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
		JobStatus status = job.getStatus();
		return status == JobStatus.STARTING || status == JobStatus.RUNNING;
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
}
