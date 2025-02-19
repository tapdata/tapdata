package io.tapdata.observable.logging;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.entity.GlobalConstant;
import io.tapdata.observable.logging.appender.*;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import io.tapdata.observable.logging.util.LogUtil;
import io.tapdata.observable.logging.with.WithAppender;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.RollingFileManager;
import org.apache.logging.log4j.core.appender.rolling.action.Action;
import org.apache.logging.log4j.core.appender.rolling.action.DeleteAction;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author Dexter
 **/
public class TaskLogger extends ObsLogger {
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
	@Getter
	private boolean enableDebugLogger;
	@Getter
	private Long recordCeiling;
	@Getter
	@Setter
	private Long intervalCeiling;
	private boolean testTask;
	public boolean isTestTask() {
		return testTask;
	}

	protected TaskLogger(TaskDto taskDto, BiConsumer<String, LogLevel> consumer) {
		String taskId = taskDto.getId().toHexString();
		this.taskId = taskId;
		this.taskName = taskDto.getName();
		this.taskRecordId = taskDto.getTaskRecordId();
		this.logAppendFactory = AppenderFactory.getInstance();
		this.testTask = taskDto.isTestTask();

		AtomicReference<Object> taskInfo = (AtomicReference<Object>) taskDto.taskInfo(ScriptNodeProcessNodeAppender.LOG_LIST_KEY + taskId);
		if (taskDto.isTestTask() && null != taskInfo){
			//js处理器试运行收集日志，不入库不额外操作，仅返回给前端
			this.witAppender((WithAppender<MonitoringLogsDto>)(() ->
				(BaseTaskAppender<MonitoringLogsDto>) ScriptNodeProcessNodeAppender.create(
							taskId, taskInfo, Optional.ofNullable((Integer) taskDto.taskInfo(ScriptNodeProcessNodeAppender.MAX_LOG_LENGTH_KEY + taskId)).orElse(100))
					.nodeID((String)taskDto.taskInfo(ScriptNodeProcessNodeAppender.SCRIPT_NODE_ID_KEY + taskId))
				)
			);

		} else if (taskDto.isPreviewTask()) {
			// when preview task, no need to add any appender
		} else {
			this.witAppender(this.fileAppender(taskId))
					.witAppender(this.obsHttpTMAppender(taskId))
					.witAppender(this.debugFileAppender(taskId));
		}

		// add close debug consumer
		this.closeDebugConsumer = consumer;
	}

	private WithAppender<MonitoringLogsDto> fileAppender(String taskId){
		return () -> {
			// add file appender
			String workDir = GlobalConstant.getInstance().getConfigurationCenter().getConfig(ConfigurationCenter.WORK_DIR).toString();
			return (BaseTaskAppender<MonitoringLogsDto>) FileAppender.create(workDir, taskId);
		};
	}

	public String getDebugFileAppenderName(String taskId) {
		return taskId + "_debug";
	}

	private WithAppender<MonitoringLogsDto> debugFileAppender(String taskId){
		return () -> {
			// add file appender
			String workDir = GlobalConstant.getInstance().getConfigurationCenter().getConfig(ConfigurationCenter.WORK_DIR).toString();
			return (BaseTaskAppender<MonitoringLogsDto>) FileAppender.create(workDir, getDebugFileAppenderName(taskId))
					.include(LogLevel.DEBUG);
		};
	}

	private WithAppender<MonitoringLogsDto> obsHttpTMAppender(String taskId){
		return () -> {
			// add tm appender
			ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
			return (BaseTaskAppender<MonitoringLogsDto>) ObsHttpTMAppender.create(clientMongoOperator, taskId);
		};
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

	static TaskLogger create(TaskDto taskDto, BiConsumer<String, LogLevel> consumer) {
		return new TaskLogger(taskDto, consumer);
	}

	TaskLogger withTaskLogSetting(String level, Long recordCeiling, Long intervalCeiling) {
		LogLevel logLevel = LogLevel.getLogLevel(level);

		if (logLevel.isDebug() && !enableDebugLogger) {
			enableDebugLogger = true;

			this.recordCeiling = recordCeiling;
			this.intervalCeiling = intervalCeiling == null ?
					System.currentTimeMillis() + INTERVAL_CEILING_DEFAULT * 1000 :
					System.currentTimeMillis() + intervalCeiling * 1000;
			openCatchData();
			return this;
		}
		if (!logLevel.isDebug() && enableDebugLogger) {
			enableDebugLogger = false;
			this.recordCeiling = null;
			this.intervalCeiling = null;
			closeCatchData();
		}
		return this;
	}

	private String formatMessage(String message, Object... params) {
		return (new ParameterizedMessage(message, params)).getFormattedMessage();
	}

	public boolean noNeedLog(String level) {
		LogLevel logLevel = Optional.ofNullable(LogLevel.getLogLevel(level)).orElse(LogLevel.TRACE);

		if (!logLevel.isDebug()) {
			return false;
		}

		if (!enableDebugLogger)
			return true;

		boolean noNeedLog = false;
		if (null != recordCeiling) {
			recordCeiling--;
			noNeedLog = recordCeiling < 0;
		}

		if (!noNeedLog && null != intervalCeiling) {
			noNeedLog = intervalCeiling < System.currentTimeMillis();
		}

		if (noNeedLog) {
			this.enableDebugLogger = false;
			this.recordCeiling = null;
			this.intervalCeiling = null;

			closeCatchData();
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

	public void trace(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {
		if (noNeedLog(LogLevel.TRACE.getLevel())) {
			return;
		}

		MonitoringLogsDto.MonitoringLogsDtoBuilder builder = call(callable);
		builder.level(Level.TRACE.toString());
		builder.message(formatMessage(message, params));

		logAppendFactory.appendLog(builder.build());
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
	public void refreshFileAppender(LogConfiguration logConfiguration) {
		Configuration config = LoggerContext.getContext(false).getConfiguration();
		for (io.tapdata.observable.logging.appender.Appender<?> appender : tapObsAppenders) {
			if (appender instanceof FileAppender) {
				RollingFileAppender rollingFileAppender = ((FileAppender) appender).getRollingFileAppender();
				RollingFileManager manager = rollingFileAppender.getManager();
				CompositeTriggeringPolicy compositeTriggeringPolicy = LogUtil.getCompositeTriggeringPolicy(logConfiguration.getLogSaveSize().toString());
				String golb = taskId + "-*.log.*.gz";
				DeleteAction deleteAction = LogUtil.getDeleteAction(logConfiguration.getLogSaveTime(), ((FileAppender) appender).getLogsPath(), golb, config);
				Action[] actions = {deleteAction};
				DefaultRolloverStrategy strategy = DefaultRolloverStrategy.newBuilder()
						.withMax(logConfiguration.getLogSaveCount().toString())
						.withCustomActions(actions)
						.withConfig(config)
						.build();
				manager.setRolloverStrategy(strategy);
				manager.setTriggeringPolicy(compositeTriggeringPolicy);
			}
		}
	}

	private boolean openCatchData() {
		AtomicBoolean result = new AtomicBoolean(false);
		filterDebugFileAppender(t -> result.set(((FileAppender)t).openCatchData()));
		return result.get();
	}

	protected boolean closeCatchData() {
		AtomicBoolean result = new AtomicBoolean(false);
		filterDebugFileAppender(t -> result.set(((FileAppender)t).closeCatchData()));
		if (null != closeDebugConsumer) {
			closeDebugConsumer.accept(taskId, LogLevel.INFO);
		}
		return result.get();
	}

	public void filterDebugFileAppender(Consumer<? super Appender<?>> fun) {
		String debugFileAppenderName = getDebugFileAppenderName(getTaskId());
		this.tapObsAppenders.stream()
				.filter(t -> t instanceof FileAppender)
				.filter(t -> debugFileAppenderName.equals( ((FileAppender) t).getTaskId() ))
				.findFirst().ifPresent(fun);
	}
}
