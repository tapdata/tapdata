package io.tapdata.observable.logging;

import com.tapdata.constant.BeanUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.common.SettingService;
import io.tapdata.observable.logging.appender.FileAppender;
import io.tapdata.observable.logging.appender.AppenderFactory;
import io.tapdata.observable.logging.appender.TMAppender;
import lombok.Getter;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.Callable;

/**
 * @author Dexter
 **/
class TaskLogger extends ObsLogger implements Serializable {
	private static AppenderFactory logAppendFactory = null;
	private static FileAppender fileAppender = null;

	@Getter
 	private String taskId;
	@Getter
	private String taskRecordId;
	@Getter
	private String taskName;

	private final SettingService settingService;

	TaskLogger() {
		settingService = BeanUtil.getBean(SettingService.class);
		if (null != logAppendFactory && null != fileAppender) {
			return;
		}

		logAppendFactory = AppenderFactory.getInstance();
		// add file appender
		fileAppender = new FileAppender("./fileObserveLogAppenderV2");
		logAppendFactory.register(fileAppender);

		// add tm appender
		ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		TMAppender tmAppender = new TMAppender(clientMongoOperator);
		logAppendFactory.register(tmAppender);
	}

	TaskLogger withTask(String taskId, String taskName, String taskRecordId) {
		this.taskId = taskId;
		this.taskName = taskName;
		this.taskRecordId = taskRecordId;
		return this;
	}

	void registerTaskFileAppender(String taskId) {
		fileAppender.addRollingFileAppender(taskId);
	}

	void unregisterTaskFileAppender(String taskId) {
		fileAppender.removeRollingFileAppender(taskId);
	}

	private String formatMessage(String message, Object... params) {
		return (new ParameterizedMessage(message, params)).getFormattedMessage();
	}

	public boolean noNeedLog(String level) {
		String settingLevel = settingService.getSetting("logLevel").getValue();
		return LogLevel.lt(level, settingLevel);
	}

	private MonitoringLogsDto.MonitoringLogsDtoBuilder call(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable) {
		try {
			return callable.call();
		} catch (Throwable throwable) {
			throw new RuntimeException(throwable);
		}
	}

	public void debug(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {
		if (noNeedLog(LogLevel.DEBUG.getLevel())) {
			return;
		}

		MonitoringLogsDto.MonitoringLogsDtoBuilder builder = call(callable);
		builder.level(Level.DEBUG.toString());
		builder.message(formatMessage(message, params));

		logAppendFactory.appendLog(builder.build());
	}

	public void info(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params){
		if (noNeedLog(LogLevel.INFO.getLevel())) {
			return;
		}

		MonitoringLogsDto.MonitoringLogsDtoBuilder builder = call(callable);
		builder.level(Level.INFO.toString());
		builder.message(formatMessage(message, params));

		logAppendFactory.appendLog(builder.build());
	}

	public void warn(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params){
		if (noNeedLog(LogLevel.WARN.getLevel())) {
			return;
		}

		MonitoringLogsDto.MonitoringLogsDtoBuilder builder = call(callable);
		builder.level(Level.WARN.toString());
		builder.message(formatMessage(message, params));

		logAppendFactory.appendLog(builder.build());
	}

	public void error(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params){
		if (noNeedLog(LogLevel.ERROR.getLevel())) {
			return;
		}

		ParameterizedMessage parameterizedMessage = new ParameterizedMessage(message, params);
		final String formattedMessage = parameterizedMessage.getFormattedMessage();

		StringBuilder errorStackSB = new StringBuilder();
		Throwable throwable = parameterizedMessage.getThrowable();
		if (throwable != null) {
			while (throwable.getCause() != null) {
				throwable = throwable.getCause();

				errorStackSB.append(throwable.getMessage()).append('\n');
				for (StackTraceElement stackTraceElement : throwable.getStackTrace()) {
					errorStackSB.append(stackTraceElement.toString()).append('\n');
				}
			}
		}

		MonitoringLogsDto.MonitoringLogsDtoBuilder builder = call(callable);
		builder.level(Level.ERROR.toString());
		builder.message(formattedMessage);
		builder.errorStack(errorStackSB.toString());

		logAppendFactory.appendLog(builder.build());
	}

	public void fatal(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params){
		if (noNeedLog(LogLevel.FATAL.getLevel())) {
			return;
		}

		ParameterizedMessage parameterizedMessage = new ParameterizedMessage(message, params);
		final String formattedMessage = parameterizedMessage.getFormattedMessage();

		StringBuilder errorStackSB = new StringBuilder();
		Throwable throwable = parameterizedMessage.getThrowable();
		if (throwable != null) {
			while (throwable.getCause() != null) {
				throwable = throwable.getCause();

				errorStackSB.append(throwable.getMessage()).append('\n');
				for (StackTraceElement stackTraceElement : throwable.getStackTrace()) {
					errorStackSB.append(stackTraceElement.toString()).append('\n');
				}
			}
		}

		MonitoringLogsDto.MonitoringLogsDtoBuilder builder = call(callable);
		builder.level(Level.FATAL.toString());
		builder.message(formattedMessage);
		builder.errorStack(errorStackSB.toString());

		logAppendFactory.appendLog(builder.build());
	}

	@NotNull
	public MonitoringLogsDto.MonitoringLogsDtoBuilder logBaseBuilder() {
		Date date = new Date();

		return MonitoringLogsDto.builder()
				.date(date)
				.timestamp(date.getTime())
				.taskId(taskId)
				.taskName(taskName)
				.taskRecordId(taskRecordId)
				;
	}
}
