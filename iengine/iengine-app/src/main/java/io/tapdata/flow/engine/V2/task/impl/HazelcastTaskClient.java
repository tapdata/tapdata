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
import io.tapdata.flow.engine.V2.common.HazelcastStatusMappingEnum;
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.util.SupplierImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Optional;

/**
 * @author jackin
 * @date 2021/12/7 9:47 PM
 **/
public class HazelcastTaskClient implements TaskClient<TaskDto> {

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

	public HazelcastTaskClient(Job job, TaskDto taskDto, ClientMongoOperator clientMongoOperator, ConfigurationCenter configurationCenter, HazelcastInstance hazelcastInstance) {
		this.job = job;
		this.taskDto = taskDto;
		this.clientMongoOperator = clientMongoOperator;
		this.configurationCenter = configurationCenter;
		this.hazelcastInstance = hazelcastInstance;
		if (!StringUtils.equalsAnyIgnoreCase(taskDto.getSyncType(), TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
			this.monitorManager = new MonitorManager();
			try {
				this.monitorManager.startMonitor(MonitorManager.MonitorType.SUBTASK_PING_TIME, taskDto, clientMongoOperator, new SupplierImpl<>(this::stop));
			} catch (Exception e) {
				logger.warn("The task ping time monitor failed to start, which may affect the ping time functionality; Error: "
						+ e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			}
			snapshotProgressManager = new SnapshotProgressManager(taskDto, clientMongoOperator);
			snapshotProgressManager.startStatsSubTaskSnapshotProgress();
		}
		Optional<Node> cacheNode = taskDto.getDag().getNodes().stream().filter(n -> n instanceof CacheNode).findFirst();
		cacheNode.ifPresent(c -> cacheName = ((CacheNode) c).getCacheName());
	}

	@Override
	public String getStatus() {
		return HazelcastStatusMappingEnum.fromJobStatus(job.getStatus());
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
	public synchronized boolean stop() {
		Optional.ofNullable(snapshotProgressManager).ifPresent(SnapshotProgressManager::close);
		if (job.getStatus() == JobStatus.RUNNING) {
			job.suspend();
		}

		if (job.getStatus() == JobStatus.SUSPENDED) {
			job.cancel();
		}

		if (job.getStatus() == JobStatus.SUSPENDED || job.getStatus() == JobStatus.FAILED) {
			try {
				monitorManager.close();
			} catch (IOException ignore) {
			}
		}
		return job.getStatus().isTerminal();
	}

	@Override
	public void join() {
		this.job.join();
	}
}
