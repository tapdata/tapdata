package io.tapdata.observable.logging;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.flow.engine.V2.entity.GlobalConstant;
import io.tapdata.observable.logging.appender.Appender;
import io.tapdata.observable.logging.appender.AppenderFactory;
import io.tapdata.observable.logging.appender.BaseTaskAppender;
import io.tapdata.observable.logging.appender.FileAppender;
import io.tapdata.observable.logging.appender.ObsHttpTMAppender;
import io.tapdata.observable.logging.with.WithAppender;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * @author Dexter
 **/
class TaskLogger extends ObsLogger {
	private static final Long RECORD_CEILING_DEFAULT = 500L;
	private static final Long INTERVAL_CEILING_DEFAULT = 500L;
	private static final long serialVersionUID = -5640539419072201312L;
	private final List<io.tapdata.observable.logging.appender.Appender<?>> tapObsAppenders = new ArrayList<>();
	private final AppenderFactory logAppendFactory;
	private final BiConsumer<String, LogLevel> closeDebugConsumer;
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

	private TaskLogger(String taskId, String taskName, String taskRecordId, BiConsumer<String, LogLevel> consumer) {
		this.taskId = taskId;
		this.taskName = taskName;
		this.taskRecordId = taskRecordId;
		this.logAppendFactory = AppenderFactory.getInstance();

		// add close debug consumer
		this.closeDebugConsumer = consumer;
	}

	public TaskLogger witAppender(WithAppender<?> appender){
		Appender<?> append = appender.append();
		tapObsAppenders.add(append);
		this.logAppendFactory.addTaskAppender((BaseTaskAppender<MonitoringLogsDto>)append);
		return this;
	}

	TaskLogger withTask(String taskId, String taskName, String taskRecordId) {
		this.taskId = taskId;
		this.taskName = taskName;
		this.taskRecordId = taskRecordId;

		return this;
	}

	static TaskLogger create(String taskId, String taskName, String taskRecordId, BiConsumer<String, LogLevel> consumer) {
		return new TaskLogger(taskId, taskName, taskRecordId, consumer);
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
		throwable = getThrowable(throwable, params);
		if (null == message && throwable != null) {
			message = throwable.getMessage();
		}
		ParameterizedMessage parameterizedMessage = new ParameterizedMessage(message, params, throwable);

		MonitoringLogsDto.MonitoringLogsDtoBuilder builder = call(callable);
		builder.level(Level.ERROR.toString());
		buildErrorMessage(throwable, parameterizedMessage, builder);

		logAppendFactory.appendLog(builder.build());
	}

	@Nullable
	private static Throwable getThrowable(Throwable throwable, Object[] params) {
		if (null == throwable) {
			throwable = findThrowable(params);
		}
		return throwable;
	}

	public void fatal(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, Throwable throwable, String message, Object... params) {
		if (noNeedLog(LogLevel.FATAL.getLevel())) {
			return;
		}
		throwable = getThrowable(throwable, params);
		if (null == message && throwable != null) {
			message = throwable.getMessage();
		}
		ParameterizedMessage parameterizedMessage = new ParameterizedMessage(message, params, throwable);

		MonitoringLogsDto.MonitoringLogsDtoBuilder builder = call(callable);
		builder.level(Level.FATAL.toString());
		buildErrorMessage(throwable, parameterizedMessage, builder);

		logAppendFactory.appendLog(builder.build());
	}

	@Override
	public boolean isInfoEnabled() {
		return isEnabled(LogLevel.INFO);
	}

	@Override
	public boolean isWarnEnabled() {
		return isEnabled(LogLevel.WARN);
	}

	@Override
	public boolean isErrorEnabled() {
		return isEnabled(LogLevel.ERROR);
	}

	@Override
	public boolean isFatalEnabled() {
		return isEnabled(LogLevel.FATAL);
	}

	@Override
	public boolean isDebugEnabled() {
		return isEnabled(LogLevel.DEBUG);
	}

	@Override
	public boolean isEnabled(LogLevel logLevel) {
		return !noNeedLog(logLevel.getLevel());
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
	public MonitoringLogsDto.MonitoringLogsDtoBuilder logBaseBuilder() {
		Date date = new Date();

		return MonitoringLogsDto.builder()
				.date(date)
				.timestamp(date.getTime())
				.taskId(taskId)
				.taskName(taskName)
				.taskRecordId(taskRecordId);
	}

	public void start() {
		if (CollectionUtils.isNotEmpty(tapObsAppenders)) {
			for (io.tapdata.observable.logging.appender.Appender<?> tapObsAppender : tapObsAppenders) {
				tapObsAppender.start();
			}
		}
	}

	public void close() throws Exception {
		if (null != logAppendFactory) {
			this.logAppendFactory.removeAppenders(taskId);
		}
		tapObsAppenders.forEach(Appender::stop);
	}
}
