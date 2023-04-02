package io.tapdata.observable.logging.appender;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.File;
import java.util.List;

/**
 * @author jackin
 * @date 2022/6/21 18:36
 **/
public class FileAppender extends BaseTaskAppender<MonitoringLogsDto> {
	public static final String LOGGER_NAME_PREFIX = "job-file-log-";
	public static final String LOG_PATH = "logs" + File.separator + "jobs";
	public static final String ONE_GB = "1G";
	private final Logger logger;
	private final String workDir;

	private FileAppender(String workDir, String taskId) {
		super(taskId);
		this.workDir = workDir;
		this.logger = LogManager.getLogger(LOGGER_NAME_PREFIX + taskId);
		Configurator.setLevel(LOGGER_NAME_PREFIX, Level.DEBUG);
	}

	public static FileAppender create(String workDir, String taskId) {
		return new FileAppender(workDir, taskId);
	}

	@Override
	public void append(MonitoringLogsDto log) {
		final String level = log.getLevel();
		switch (level) {
			case "DEBUG":
				logger.debug(log.formatMonitoringLogMessage());
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

		final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		final Configuration config = ctx.getConfiguration();
		PatternLayout patternLayout = PatternLayout.newBuilder()
				.withPattern("[%-5level] %date{yyyy-MM-dd HH:mm:ss.SSS} - %msg%n")
				.withConfiguration(config)
				.build();

		TimeBasedTriggeringPolicy timeBasedTriggeringPolicy = TimeBasedTriggeringPolicy.newBuilder().withInterval(1).withModulate(true).build();
		SizeBasedTriggeringPolicy sizeBasedTriggeringPolicy = SizeBasedTriggeringPolicy.createPolicy(ONE_GB);
		CompositeTriggeringPolicy compositeTriggeringPolicy = CompositeTriggeringPolicy.createPolicy(timeBasedTriggeringPolicy, sizeBasedTriggeringPolicy);
		DefaultRolloverStrategy strategy = DefaultRolloverStrategy.newBuilder()
				.withConfig(config).build();

		RollingFileAppender rollingFileAppender = RollingFileAppender.newBuilder()
				.setName("rollingFileAppender-" + taskId)
				.withFileName(logsPath + File.separator + taskId + ".log")
				.withFilePattern(logsPath + File.separator + taskId + ".log.%d{yyyyMMdd}-%i.gz")
				.setLayout(patternLayout)
				.withPolicy(compositeTriggeringPolicy)
				.withStrategy(strategy)
				.build();
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
	}
}
