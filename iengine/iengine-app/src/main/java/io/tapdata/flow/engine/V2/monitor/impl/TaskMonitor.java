package io.tapdata.flow.engine.V2.monitor.impl;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.monitor.Monitor;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;

/**
 * @author samuel
 * @Description
 * @create 2022-03-02 01:31
 **/
public abstract class TaskMonitor<T> implements Monitor<T> {

	private static final Long INTERVAL_MS = 2000L;

	protected Long intervalMs;
	protected TaskDto taskDto;
	protected ObsLogger logger;

	public TaskMonitor(TaskDto taskDto) {
		assert null != taskDto;
		this.taskDto = taskDto;
		this.intervalMs = INTERVAL_MS;
		this.logger = ObsLoggerFactory.getInstance().getObsLogger(taskDto);
	}

	@Override
	public T get() {
		return null;
	}
}
