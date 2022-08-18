package io.tapdata.observable.logging;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.Callable;

/**
 * @author Dexter
 **/
class TaskLoggerNodeProxy extends ObsLogger implements Serializable {
	private TaskLogger taskLogger;
	private String nodeId;
	private String nodeName;

	TaskLoggerNodeProxy withTaskLogger(TaskLogger taskLogger) {
		this.taskLogger = taskLogger;
		return this;
	}

	TaskLoggerNodeProxy withNode(String nodeId, String nodeName) {
		this.nodeId = nodeId;
		this.nodeName = nodeName;
		return this;
	}

	public void debug(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {
		taskLogger.debug(callable, message, params);
	}

	public void info(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {
		taskLogger.info(callable, message, params);
	}

	public void warn(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {
		taskLogger.warn(callable, message, params);
	}

	@Override
	public void error(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {
		taskLogger.error(callable, message, params);
	}

	@Override
	public void fatal(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {
		taskLogger.fatal(callable, message, params);
	}

	@NotNull
	public MonitoringLogsDto.MonitoringLogsDtoBuilder logBaseBuilder() {
		Date date = new Date();

		return MonitoringLogsDto.builder()
				.date(date)
				.timestamp(date.getTime())
				.taskId(taskLogger.getTaskId())
				.taskName(taskLogger.getTaskName())
				.taskRecordId(taskLogger.getTaskRecordId())
				.nodeId(nodeId)
				.nodeName(nodeName)
				;
	}
}
