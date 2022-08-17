package io.tapdata.flow.engine.V2.monitor.impl;

import com.tapdata.constant.ExecutorUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.milestone.MilestoneFlowServiceJetV2;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author samuel
 * @Description
 * @create 2022-03-08 18:53
 **/
public class TaskMilestoneMonitor extends TaskMonitor<Object> {

	private MilestoneFlowServiceJetV2 milestoneService;
	private ScheduledExecutorService executorService;

	public TaskMilestoneMonitor(TaskDto taskDto, MilestoneFlowServiceJetV2 milestoneService) {
		super(taskDto);
		this.milestoneService = milestoneService;
		this.executorService = new ScheduledThreadPoolExecutor(1);
	}

	@Override
	public void start() {
		executorService.scheduleAtFixedRate(() -> milestoneService.updateList(), intervalMs, intervalMs, TimeUnit.MILLISECONDS);
	}

	@Override
	public void close() throws IOException {
		ExecutorUtil.shutdown(executorService, 5L, TimeUnit.SECONDS);
	}
}
