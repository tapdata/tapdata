package io.tapdata.observable.logging.appender;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.log.CustomPatternLayout;
import io.tapdata.observable.logging.LogLevel;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.debug.DataCacheFactory;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import io.tapdata.observable.logging.util.LogUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.action.Action;
import org.apache.logging.log4j.core.appender.rolling.action.DeleteAction;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jackin
 * @date 2022/6/21 18:36
 **/
public class FileAppender extends BaseTaskAppender<MonitoringLogsDto> {
	public static final String LOGGER_NAME_PREFIX = "job-file-log-";
	public static final String LOG_PATH = "logs" + File.separator + "jobs";
	private ObsLoggerFactory obsLoggerFactory=ObsLoggerFactory.getInstance();
	public static final String ONE_GB = "1G";
	private final Logger logger;
	private final String workDir;
	private String logsPath;
	private Set<String> includeLogLevel;
	private AtomicBoolean catchData = new AtomicBoolean(false);

	public RollingFileAppender getRollingFileAppender() {
		return rollingFileAppender;
	}

	private RollingFileAppender rollingFileAppender;

	private FileAppender(String workDir, String taskId) {
		super(taskId);
		this.workDir = workDir;
		this.logger = LogManager.getLogger(LOGGER_NAME_PREFIX + taskId);
		Configurator.setLevel(LOGGER_NAME_PREFIX + taskId, Level.TRACE);
	}

	public String getLogsPath() {
		return logsPath;
	}
	public static FileAppender create(String workDir, String taskId) {
		return new FileAppender(workDir, taskId);
	}

	@Override
	public void append(MonitoringLogsDto log) {
		final String level = log.getLevel();
		if (CollectionUtils.isNotEmpty(includeLogLevel) && !includeLogLevel.contains(level))
			return;
		switch (level) {
			case "DEBUG":
				if (catchData.get()) {
					if (CollectionUtils.isEmpty(log.getLogTags()) || !log.getLogTags().contains("catchData"))
						return;
					logger.debug(log.formatMonitoringLogMessage());
					String taskId = getTaskId();
					if (taskId != null) taskId = taskId.replace("_debug", "");
					DataCacheFactory.getInstance().getDataCache(taskId).put(log);
				}
				break;
			case "INFO":
				logger.info(log.formatMonitoringLogMessage());
				break;
			case "WARN":
				logger.warn(log.formatMonitoringLogMessage());
				break;
			case "ERROR":
				logger.error(log.formatMonitoringLogMessage());
				break;
			case "FATAL":
				logger.fatal(log.formatMonitoringLogMessage());
				break;
			case "TRACE":
				logger.trace(log.formatMonitoringLogMessage());
				break;
		}
	}

	@Override
	public void append(List<MonitoringLogsDto> logs) {
		for (MonitoringLogsDto log : logs) {
			append(log);
		}
	}

	@Override
	public void start() {
		StringBuilder logsPath = new StringBuilder();
		if (StringUtils.isNotBlank(workDir)) {
			logsPath.append(workDir).append(File.separator).append(LOG_PATH);
		} else {
			logsPath.append(LOG_PATH);
		}
		this.logsPath = logsPath.toString();
		LogConfiguration logConfiguration = obsLoggerFactory.getLogConfiguration("task");

		final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		final Configuration config = ctx.getConfiguration();
		CustomPatternLayout patternLayout = CustomPatternLayout.newBuilder()
				.withPattern("[%-5level] %date{yyyy-MM-dd HH:mm:ss.SSS} - %msg%n")
				.withConfiguration(config)
				.build();
		CompositeTriggeringPolicy compositeTriggeringPolicy = LogUtil.getCompositeTriggeringPolicy(logConfiguration.getLogSaveSize().toString());
		String golb = taskId + "-*.log.*.gz";
		DeleteAction deleteAction = LogUtil.getDeleteAction(logConfiguration.getLogSaveTime(), logsPath.toString(), golb, config);
		Action[] actions = {deleteAction};


		DefaultRolloverStrategy strategy = DefaultRolloverStrategy.newBuilder()
				.withMax(logConfiguration.getLogSaveCount().toString())
				.withCustomActions(actions)
				.withConfig(config).build();

		RollingFileAppender rollingFileAppender = RollingFileAppender.newBuilder()
				.setName("rollingFileAppender-" + taskId)
				.withFileName(logsPath + File.separator + taskId + ".log")
				.withFilePattern(logsPath + File.separator + taskId + "-%i.log.%d{yyyyMMdd}.gz")
				.setLayout(patternLayout)
				.withPolicy(compositeTriggeringPolicy)
				.withStrategy(strategy)
				.build();
		this.rollingFileAppender = rollingFileAppender;
		rollingFileAppender.start();
		org.apache.logging.log4j.core.Logger coreLogger = (org.apache.logging.log4j.core.Logger) logger;
		coreLogger.addAppender(rollingFileAppender);
//		coreLogger.setAdditive(false);
	}

	@Override
	public void stop() {
		if (null != logger) {
			removeAppenders((org.apache.logging.log4j.core.Logger) logger);
		}
		DataCacheFactory.getInstance().removeDataCache(getTaskId());
	}

	public FileAppender include(LogLevel... level) {
		if (level.length == 0) {
			return this;
		}
		if (includeLogLevel == null) {
			includeLogLevel = new HashSet<>();
		}
		Arrays.stream(level).map(LogLevel::getLevel).forEach(includeLogLevel::add);
		return this;
	}

	public boolean openCatchData() {
		DataCacheFactory.getInstance().removeDataCache(getTaskId());
		catchData.set(true);
		return true;
	}

	public boolean closeCatchData() {
		catchData.set(false);
		DataCacheFactory.getInstance().removeDataCache(getTaskId());
		return true;
	}

	public FileAppender include(LogLevel... level) {
		if (level.length == 0) {
			return this;
		}
		if (includeLogLevel == null) {
			includeLogLevel = new HashSet<>();
		}
		Arrays.stream(level).map(LogLevel::getLevel).forEach(includeLogLevel::add);
		return this;
	}
}
