package io.tapdata.flow.engine.V2.task.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.flow.engine.V2.common.HazelcastStatusMappingEnum;
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.milestone.MilestoneService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Optional;

/**
 * @author jackin
 * @date 2021/12/7 9:47 PM
 **/
public class HazelcastTaskClient implements TaskClient<SubTaskDto> {

	private Logger logger = LogManager.getLogger(HazelcastTaskClient.class);

	private Job job;
	private SubTaskDto subTaskDto;
	//  private BaseMetrics taskMetrics;
//  protected ScheduledExecutorService metricsThreadPool;
//  protected ScheduledFuture<?> metricsThreadPoolFuture;
	private ConfigurationCenter configurationCenter;
	private ClientMongoOperator clientMongoOperator;
	private HazelcastInstance hazelcastInstance;
	private MonitorManager monitorManager;
	private SnapshotProgressManager snapshotProgressManager;
	private String cacheName;

	public HazelcastTaskClient(Job job, SubTaskDto subTaskDto, ClientMongoOperator clientMongoOperator, ConfigurationCenter configurationCenter, HazelcastInstance hazelcastInstance, MilestoneService milestoneService) {
		this.job = job;
		this.subTaskDto = subTaskDto;
		this.clientMongoOperator = clientMongoOperator;
		this.configurationCenter = configurationCenter;
		this.hazelcastInstance = hazelcastInstance;
		if (!subTaskDto.isTransformTask()) {
			this.monitorManager = new MonitorManager();
			try {
				this.monitorManager.startMonitor(MonitorManager.MonitorType.SUBTASK_MILESTONE_MONITOR, subTaskDto, milestoneService);
			} catch (Exception e) {
				logger.warn("The milestone monitor failed to start, which may affect the milestone functionality; Error: "
						+ e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			}
			try {
				this.monitorManager.startMonitor(MonitorManager.MonitorType.SUBTASK_PING_TIME, subTaskDto, clientMongoOperator);
			} catch (Exception e) {
				logger.warn("The task ping time monitor failed to start, which may affect the ping time functionality; Error: "
						+ e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			}
			snapshotProgressManager = new SnapshotProgressManager(subTaskDto, clientMongoOperator);
			snapshotProgressManager.startStatsSubTaskSnapshotProgress();
		}
		Optional<Node> cacheNode = subTaskDto.getDag().getNodes().stream().filter(n -> n instanceof CacheNode).findFirst();
		cacheNode.ifPresent(c -> cacheName = ((CacheNode) c).getCacheName());
	}

	@Override
	public String getStatus() {
		return HazelcastStatusMappingEnum.fromJobStatus(job.getStatus());
	}

	@Override
	public SubTaskDto getTask() {
		return subTaskDto;
	}

	@Override
	public String getCacheName() {
		return cacheName;
	}

	@Override
	public boolean stop() {
		Optional.ofNullable(snapshotProgressManager).ifPresent(SnapshotProgressManager::close);
		if (job.getStatus() == JobStatus.RUNNING) {
			job.suspend();
		}

		if (job.getStatus() == JobStatus.SUSPENDED) {
			job.cancel();

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
