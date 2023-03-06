package io.tapdata.observable.logging;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.flow.engine.V2.entity.GlobalConstant;
import io.tapdata.observable.logging.appender.AppenderFactory;
import io.tapdata.observable.logging.appender.FileAppender;
import io.tapdata.observable.logging.appender.TMAppender;
import lombok.Getter;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * @author Dexter
 **/
class TaskLogger extends ObsLogger implements Serializable {
	private static final Long RECORD_CEILING_DEFAULT = 500L;
	private static final Long INTERVAL_CEILING_DEFAULT = 500L;

	private static AppenderFactory logAppendFactory = null;
	private static FileAppender fileAppender = null;
	private static BiConsumer<String, LogLevel> closeDebugConsumer;

	@Getter
	private String taskId;
	@Getter
	private String taskRecordId;
	@Getter
	private String taskName;

	private LogLevel level;
	private LogLevel formerLevel;
	private Long recordCeiling;
	private Long intervalCeiling;

	TaskLogger(BiConsumer<String, LogLevel> consumer) {
		if (null != logAppendFactory && null != fileAppender && closeDebugConsumer != null) {
			return;
		}

		logAppendFactory = AppenderFactory.getInstance();
		// add file appender
		fileAppender = new FileAppender(GlobalConstant.getInstance().getConfigurationCenter().getConfig(ConfigurationCenter.WORK_DIR).toString());
		logAppendFactory.register(fileAppender);

		// add tm appender
		ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		TMAppender tmAppender = new TMAppender(clientMongoOperator);
		logAppendFactory.register(tmAppender);

		// add close debug consumer
		closeDebugConsumer = consumer;
	}

	TaskLogger withTask(String taskId, String taskName, String taskRecordId) {
		this.taskId = taskId;
		this.taskName = taskName;
		this.taskRecordId = taskRecordId;

		return this;
	}

	TaskLogger withTaskLogSetting(String level, Long recordCeiling, Long intervalCeiling) {
		LogLevel logLevel = LogLevel.getLogLevel(level);
		if (this.level == logLevel) {
			return this;
		}
		this.formerLevel = this.level;
		this.level = logLevel;
		if (this.level.isDebug()) {
			if (null == recordCeiling && null == intervalCeiling) {
				this.recordCeiling = RECORD_CEILING_DEFAULT;
				this.intervalCeiling = System.currentTimeMillis() + INTERVAL_CEILING_DEFAULT * 1000;
				return this;
			}

			this.recordCeiling = recordCeiling;
			if (intervalCeiling != null) {
				this.intervalCeiling = System.currentTimeMillis() + intervalCeiling * 1000;
			}
			return this;
		}

		this.recordCeiling = null;
		this.intervalCeiling = null;
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
		if (!this.level.isDebug()) {
			return !this.level.shouldLog(level);
		}

		boolean noNeedLog = false;
		if (null != recordCeiling) {
			recordCeiling--;
			noNeedLog = recordCeiling <= 0;
		}

		if (!noNeedLog && null != intervalCeiling) {
			noNeedLog = intervalCeiling < System.currentTimeMillis();
		}

		if (noNeedLog && this.level.isDebug()) {
			// fix NPE when task start as DEBUG level
			if (null == formerLevel) {
				formerLevel = LogLevel.INFO;
			}
			this.level = formerLevel;
			this.recordCeiling = null;
			this.intervalCeiling = null;
			if (null != closeDebugConsumer) {
				closeDebugConsumer.accept(taskId, formerLevel);
			}
		}

		return noNeedLog;
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

	public void info(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {
		if (noNeedLog(LogLevel.INFO.getLevel())) {
			return;
		}

		MonitoringLogsDto.MonitoringLogsDtoBuilder builder = call(callable);
		builder.level(Level.INFO.toString());
		builder.message(formatMessage(message, params));

		logAppendFactory.appendLog(builder.build());
	}

	public void warn(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {
		if (noNeedLog(LogLevel.WARN.getLevel())) {
			return;
		}

		MonitoringLogsDto.MonitoringLogsDtoBuilder builder = call(callable);
		builder.level(Level.WARN.toString());
		builder.message(formatMessage(message, params));

		logAppendFactory.appendLog(builder.build());
	}

	public void error(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, Throwable throwable, String message, Object... params) {
		if (noNeedLog(LogLevel.ERROR.getLevel())) {
			return;
		}
		if (null == throwable) {
			throwable = findThrowable(params);
		}
		if (null == message && throwable != null) {
			message = throwable.getMessage();
		}
		ParameterizedMessage parameterizedMessage = new ParameterizedMessage(message, params, throwable);

		MonitoringLogsDto.MonitoringLogsDtoBuilder builder = call(callable);
		builder.level(Level.ERROR.toString());
		builder.message(parameterizedMessage.getFormattedMessage());
		builder.errorStack(getErrorStack(throwable));

		logAppendFactory.appendLog(builder.build());
	}

	public void fatal(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, Throwable throwable, String message, Object... params) {
		if (noNeedLog(LogLevel.FATAL.getLevel())) {
			return;
		}
		if (null == throwable) {
			throwable = findThrowable(params);
		}
		if (null == message && throwable != null) {
			message = throwable.getMessage();
		}
		ParameterizedMessage parameterizedMessage = new ParameterizedMessage(message, params, throwable);

		MonitoringLogsDto.MonitoringLogsDtoBuilder builder = call(callable);
		builder.level(Level.FATAL.toString());
		builder.message(parameterizedMessage.getFormattedMessage());
		builder.errorStack(getErrorStack(throwable));

		logAppendFactory.appendLog(builder.build());
	}

	@Nullable
	private static Throwable findThrowable(Object[] params) {
		Throwable throwable = null;
		for (Object param : params) {
			if (param instanceof Throwable) {
				throwable = (Throwable) param;
				break;
			}
		}
		return throwable;
	}

	@NotNull
	private static String getErrorStack(Throwable throwable) {
		StringBuilder errorStackSB = new StringBuilder();
		while (throwable != null) {
			errorStackSB.append(throwable.getMessage()).append('\n');
			for (StackTraceElement stackTraceElement : throwable.getStackTrace()) {
				errorStackSB.append("  ").append(stackTraceElement.toString()).append('\n');
			}
			throwable = throwable.getCause();
		}
		return errorStackSB.toString();
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
