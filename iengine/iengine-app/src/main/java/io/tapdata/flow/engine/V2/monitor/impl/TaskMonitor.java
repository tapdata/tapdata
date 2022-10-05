package io.tapdata.flow.engine.V2.monitor.impl;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.monitor.Monitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author samuel
 * @Description
 * @create 2022-03-02 01:31
 **/
public abstract class TaskMonitor<T> implements Monitor<T> {

	private static final Long INTERVAL_MS = 2000L;

	protected Long intervalMs;
	protected TaskDto taskDto;
	protected Logger logger = LogManager.getLogger(TaskMonitor.class);

	public TaskMonitor(TaskDto taskDto) {
		assert null != taskDto;
		this.taskDto = taskDto;
		Log4jUtil.setThreadContext(taskDto);
		this.intervalMs = INTERVAL_MS;
	}

	@Override
	public T get() {
		return null;
	}
}
