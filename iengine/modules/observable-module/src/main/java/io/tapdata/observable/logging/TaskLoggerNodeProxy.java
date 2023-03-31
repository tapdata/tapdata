package io.tapdata.observable.logging;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import org.jetbrains.annotations.NotNull;

import java.util.Date;
import java.util.concurrent.Callable;

/**
 * @author Dexter
 **/
class TaskLoggerNodeProxy extends ObsLogger {
	private static final long serialVersionUID = 1049697486049209441L;
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
	public void error(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, Throwable throwable, String message, Object... params) {
		taskLogger.error(callable, throwable, message, params);
	}

	@Override
	public void fatal(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, Throwable throwable, String message, Object... params) {
		taskLogger.fatal(callable, throwable, message, params);
	}

	@Override
	public boolean isEnabled(LogLevel logLevel) {
		return taskLogger.isEnabled(logLevel);
	}

	@Override
	public boolean isInfoEnabled() {
		return taskLogger.isInfoEnabled();
	}

	@Override
	public boolean isWarnEnabled() {
		return taskLogger.isWarnEnabled();
	}

	@Override
	public boolean isErrorEnabled() {
		return taskLogger.isErrorEnabled();
	}

	@Override
	public boolean isDebugEnabled() {
		return taskLogger.isDebugEnabled();
	}

	@Override
	public boolean isFatalEnabled() {
		return taskLogger.isFatalEnabled();
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
