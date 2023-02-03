/*
 * Copyright (c) 2016, 2025, HHLY and/or its affiliates. All rights reserved.
 * HHLY PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package io.tapdata;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.StartResultUtil;
import com.tapdata.tm.sdk.util.EnvUtil;
import io.tapdata.aspect.ApplicationStartAspect;
import io.tapdata.aspect.task.AspectTaskManager;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.pdk.core.runtime.TapRuntime;
import io.tapdata.pdk.core.utils.CommonUtils;
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
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.util.IOUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.Security;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author huangjq
 * @ClassName: Application
 * @Description: TODO
 * @date 2017年5月8日 下午5:20:17
 * @since 1.0
 */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, HibernateJpaAutoConfiguration.class, MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
@Configuration
@Component
@EnableScheduling
public class Application {

	private static final String TAG = Application.class.getSimpleName();
	private static Logger logger = LogManager.getLogger(Application.class);
	private static Logger pdkLogger = LogManager.getLogger("PDK");

	private static ConfigurationCenter configurationCenter;

	private static String tapdataWorkDir;

	public static void main(String[] args) {
		CommonUtils.setProperty("tap_verbose", "true");
		EnvUtil.decodeToken(System.getenv("CLOUD_TOKEN"));
		try {
			System.setProperty(LoggingSystem.class.getName(), "none");
			tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");

			addRollingFileAppender(tapdataWorkDir);
			initSecurityConfig();
			String buildInfoJson;
			try (
					InputStream in = Application.class.getResourceAsStream("/build-info.json")
			) {
				BufferedReader reader = new BufferedReader(new InputStreamReader(in));
				buildInfoJson = IOUtils.toString(reader);
			}
			final Map<String, Object> buildInfo = JSONUtil.json2Map(buildInfoJson);
			SimpleDateFormat parse = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			Calendar now = Calendar.getInstance();
			parse.setTimeZone(now.getTimeZone());
			Date datetime = parse.parse((String) buildInfo.get("timestamp"));
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			String formatVersion = sdf.format(datetime);

			logger.info("Starting application, code version {}", formatVersion);
			SpringApplication springApplication = new SpringApplication(Application.class);
			springApplication.setWebApplicationType(WebApplicationType.NONE);
			ConfigurableApplicationContext run = springApplication.run(args);

			TapLogger.setLogListener(new TapLogger.LogListener() {
				@Override
				public void debug(String log) {
					pdkLogger.debug(log);
//					System.out.println(log);
				}

				@Override
				public void info(String log) {
					pdkLogger.info(log);
//					System.out.println(log);
				}

				@Override
				public void warn(String log) {
					pdkLogger.warn(log);
//					System.out.println(log);
				}

				@Override
				public void error(String log) {
					pdkLogger.error(log);
				}

				@Override
				public void fatal(String log) {
					pdkLogger.fatal(log);
				}

				@Override
				public void memory(String memoryLog) {
					pdkLogger.info(memoryLog);
				}
			});

			TapRuntime.getInstance();
			configurationCenter = run.getBean(ConfigurationCenter.class);
			configurationCenter.putConfig("version", "@env.VERSION@".equals(buildInfo.get("version")) ? "-" : buildInfo.get("version"));
			configurationCenter.putConfig("gitCommitId", "@env.DAAS_GIT_VERSION@".equals(buildInfo.get("gitCommitId")) ? "-" : buildInfo.get("gitCommitId"));
			if (StringUtils.isNotBlank(tapdataWorkDir)) {
				configurationCenter.putConfig(ConfigurationCenter.WORK_DIR, tapdataWorkDir);
			}
			BeanUtil.configurableApplicationContext = run;

			TapLogger.info(TAG, "Looking for Aspect annotations...");
			long time = System.currentTimeMillis();
			//Initialize AspectTasks
			InstanceFactory.instance(AspectTaskManager.class);
			TapLogger.info(TAG, "Looking for Aspect annotations takes " + (System.currentTimeMillis() - time));

			try {
				StartResultUtil.writeStartResult(tapdataWorkDir, configurationCenter.getConfig("version").toString(), null);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}

			AspectUtils.executeAspect(ApplicationStartAspect.class, ApplicationStartAspect::new);
//			run.getBean(TapdataTaskScheduler.class).stopTaskIfNeed();
			run.getBean(TapdataTaskScheduler.class).runTaskIfNeedWhenEngineStart();
		} catch (Exception e) {
			String err = "Run flow engine application failed, err: " + e.getMessage();
			logger.error(err, e);
			try {
				StartResultUtil.writeStartResult(tapdataWorkDir,
						(configurationCenter == null || configurationCenter.getConfig("version") == null) ? "-" : configurationCenter.getConfig("version").toString(), e);
			} catch (Exception exception) {
				logger.error("Write start result failed, cause: " + exception.getMessage(), exception);
			}
			System.exit(1);
		}
	}

	/**
	 * 初始化jdk安全配置
	 */
	private static void initSecurityConfig() {

		String disabledAlgorithms = AccessController.doPrivileged((PrivilegedAction<String>) () -> Security.getProperty("jdk.tls.disabledAlgorithms"));
		String[] disabledAlgorithmArray = null;
		if (disabledAlgorithms != null && !disabledAlgorithms.isEmpty()) {
			if (disabledAlgorithms.length() >= 2 && disabledAlgorithms.charAt(0) == '"' && disabledAlgorithms.charAt(disabledAlgorithms.length() - 1) == '"') {
				disabledAlgorithms = disabledAlgorithms.substring(1, disabledAlgorithms.length() - 1);
			}

			disabledAlgorithmArray = disabledAlgorithms.split(",");

			for (int index = 0; index < disabledAlgorithmArray.length; ++index) {
				disabledAlgorithmArray[index] = disabledAlgorithmArray[index].trim();
			}
		}
		List<String> disabledAlgorithmList = (List) (disabledAlgorithmArray == null ? Collections.emptyList() : new ArrayList(Arrays.asList(disabledAlgorithmArray)));

		if (!disabledAlgorithmList.isEmpty()) {
			disabledAlgorithmList.remove("TLSv1");
			disabledAlgorithmList.remove("TLSv1.1");
			String newDisabledAlgorithms = disabledAlgorithmList.stream().collect(Collectors.joining(", "));
			logger.info("disabledAlgorithms [{}]->[{}]", disabledAlgorithms, newDisabledAlgorithms);
			Security.setProperty("jdk.tls.disabledAlgorithms", newDisabledAlgorithms);
		}

	}


	@Bean
	public TaskScheduler taskScheduler() {
		final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.setPoolSize(50);
		return scheduler;
	}

	private static void addRollingFileAppender(String tapdataWorkDir) {

		Level defaultLogLevel = Level.INFO;
		String debug = System.getenv("DEBUG");
		if ("true".equalsIgnoreCase(debug)) {
			defaultLogLevel = Level.DEBUG;
		}

//        org.apache.logging.log4j.core.Logger rootLogger = (org.apache.logging.log4j.core.Logger) LogManager.getRootLogger();
//        org.apache.logging.log4j.core.config.Configuration configuration = rootLogger.getContext().getConfiguration();

		StringBuilder logsPath = new StringBuilder();
		// 优先从环境变量取日志存放目录
		String outPutLogPath = System.getenv("OUTPUT_LOG_PATH");
		if (StringUtils.isNotBlank(outPutLogPath)) {
			logsPath.append(outPutLogPath);
		} else {
			if (StringUtils.isNotBlank(tapdataWorkDir)) {
				logsPath.append(tapdataWorkDir).append("/logs");
			} else {
				logsPath.append("logs");
			}
		}

		final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		final org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
		PatternLayout patternLayout = PatternLayout.newBuilder()
				.withPattern("[%-5level] %date{yyyy-MM-dd HH:mm:ss.SSS} %X{taskId} [%t] %c{1} - %msg%n")
				.build();

		TimeBasedTriggeringPolicy timeBasedTriggeringPolicy = TimeBasedTriggeringPolicy.newBuilder().withInterval(1).withModulate(true).build();
		SizeBasedTriggeringPolicy sizeBasedTriggeringPolicy = SizeBasedTriggeringPolicy.createPolicy("1GB");
		CompositeTriggeringPolicy compositeTriggeringPolicy = CompositeTriggeringPolicy.createPolicy(timeBasedTriggeringPolicy, sizeBasedTriggeringPolicy);
		DefaultRolloverStrategy strategy = DefaultRolloverStrategy.newBuilder()
				.withMax("30")
				.withConfig(config).build();

		RollingFileAppender rollingFileAppender = RollingFileAppender.newBuilder()
				.setName("rollingFileAppender")
				.withFileName(logsPath.toString() + "/tapdata-agent.log")
				.withFilePattern(logsPath.toString() + "/tapdata-agent.log.%d{yyyyMMdd}.gz")
				.setLayout(patternLayout)
				.withPolicy(compositeTriggeringPolicy)
				.withStrategy(strategy)
				.build();
		rollingFileAppender.start();
		config.addAppender(rollingFileAppender);
		LoggerConfig rootLoggerConfig = config.getRootLogger();
		rootLoggerConfig.setLevel(defaultLogLevel);
		rootLoggerConfig.addAppender(rollingFileAppender, null, null);

		/*ConsoleAppender consoleAppender = ConsoleAppender.newBuilder()
			.withName("consoleAppender")
			.withLayout(patternLayout)
			.withImmediateFlush(true)
			.build();
		rootLoggerConfig.addAppender(consoleAppender, defaultLogLevel, null);*/

		ctx.updateLoggers();
	}

	private static boolean checkBaseURLs() throws FileNotFoundException {
		Yaml yaml = new Yaml();
		InputStream inputStream = null;
		try {
			try {
				inputStream = new FileInputStream("application.yml");
			} catch (FileNotFoundException e) {
				inputStream = new FileInputStream("connector/connector-manager/src/main/resources/application.yml");
			}
			Map<String, Object> applicationMap = yaml.load(inputStream);
			Map<String, Object> tapdata = (Map<String, Object>) applicationMap.get("com/tapdata");
			if (tapdata != null) {
				Map<String, Object> cloud = (Map<String, Object>) tapdata.get("cloud");
				if (cloud != null) {
					String baseURLs = (String) cloud.get("baseURLs");
					if (StringUtils.isBlank(baseURLs)) {
						return false;
					}
				}
			}
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					// do nothing
				}
			}
		}

		return true;
	}
}
