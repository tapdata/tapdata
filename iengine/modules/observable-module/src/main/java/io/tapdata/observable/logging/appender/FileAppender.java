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
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.File;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jackin
 * @date 2022/6/21 18:36
 **/
public class FileAppender implements Appender<MonitoringLogsDto>, Serializable {
	private final static String LOGGER_NAME_PREFIX = "observe-logger-name";
	public static final String LOG_PATH = "logs" + File.separator + "jobs";

	private Logger logger;
	private String workDir;
	private Map<String, RollingFileAppender> rollingFileAppenderMap;

	public FileAppender(String workDir) {
		this.workDir = workDir;
		this.logger = LogManager.getLogger(LOGGER_NAME_PREFIX);
		Configurator.setLevel(LOGGER_NAME_PREFIX, Level.DEBUG);
	}

	@Override
	public void append(List<MonitoringLogsDto> logs) {
		for (MonitoringLogsDto log : logs) {
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
	}

	public void addRollingFileAppender(String taskId) {
		StringBuilder logsPath = new StringBuilder();
		if (StringUtils.isNotBlank(workDir)) {
			logsPath.append(workDir).append(LOG_PATH);
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
		CompositeTriggeringPolicy compositeTriggeringPolicy = CompositeTriggeringPolicy.createPolicy(timeBasedTriggeringPolicy);
		DefaultRolloverStrategy strategy = DefaultRolloverStrategy.newBuilder()
				.withConfig(config).build();

		RollingFileAppender rollingFileAppender = RollingFileAppender.newBuilder()
				.withName("rollingFileAppender-" + taskId)
				.withFileName(logsPath + "/observe-log-" + taskId + ".log")
				.withFilePattern(logsPath + "/observe-log-" + taskId + ".log.%d{yyyyMMdd}-%i.gz")
				.withLayout(patternLayout)
				.withPolicy(compositeTriggeringPolicy)
				.withStrategy(strategy)
				.build();
		rollingFileAppender.start();
		org.apache.logging.log4j.core.Logger coreLogger = (org.apache.logging.log4j.core.Logger) logger;
		coreLogger.addAppender(rollingFileAppender);
		coreLogger.setAdditive(false);

		if (null == rollingFileAppenderMap) {
			rollingFileAppenderMap = new ConcurrentHashMap<>();
		}
		rollingFileAppenderMap.putIfAbsent(taskId, rollingFileAppender);
	}

	public void removeRollingFileAppender(String taskId) {
		rollingFileAppenderMap.computeIfPresent(taskId, (k, v) -> {
			org.apache.logging.log4j.core.Logger coreLogger = (org.apache.logging.log4j.core.Logger) logger;
			RollingFileAppender appender = rollingFileAppenderMap.get(k);
			appender.stop();
			appender.getManager().close();
			coreLogger.removeAppender(appender);

			return null;
		});
	}
}
