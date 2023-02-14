package io.tapdata.flow.engine.V2.monitor.impl;

import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JobProxy;
import io.tapdata.flow.engine.V2.monitor.Monitor;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author samuel
 * @Description
 * @create 2023-01-18 17:58
 **/
public class JetJobStatusMonitor implements Monitor<JobStatus> {
	private ScheduledExecutorService scheduledExecutorService;
	private JobProxy jetJob;
	private String nodeId;
	private JobStatus jobStatus;

	public JetJobStatusMonitor(JobProxy jetJob, String nodeId) {
		if (null == jetJob) {
			throw new IllegalArgumentException("Jet job cannot be null");
		}
		this.jetJob = jetJob;
		this.nodeId = nodeId;
	}

	@Override
	public void close() throws IOException {
		Optional.ofNullable(scheduledExecutorService).ifPresent(ExecutorService::shutdownNow);
	}

	@Override
	public void start() {
		scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
		scheduledExecutorService.scheduleAtFixedRate(() -> {
			Thread.currentThread().setName("Fresh-Jet-Job-Status-" + jetJob.getName() + "-" + nodeId);
			if (null != jetJob) {
				jobStatus = jetJob.getStatus();
			}
		}, 2, 2, TimeUnit.SECONDS);
	}

	@Override
	public JobStatus get() {
		return jobStatus;
	}
}
