package io.tapdata.observable.logging;


import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.SettingService;
import io.tapdata.common.executor.ExecutorsManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Dexter
 **/
@Slf4j
public final class ObsLoggerFactory {
	private final Logger logger = LogManager.getLogger(ObsLoggerFactory.class);

	private volatile static ObsLoggerFactory INSTANCE;

	public static ObsLoggerFactory getInstance() {
		if (INSTANCE == null) {
			synchronized (ObsLoggerFactory.class) {
				if (INSTANCE == null) {
					INSTANCE = new ObsLoggerFactory();
				}
			}
		}
		return INSTANCE;
	}

	private ObsLoggerFactory() {
		this.settingService = BeanUtil.getBean(SettingService.class);
		this.clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		this.scheduleExecutorService = ExecutorsManager.getInstance()
				.newSingleThreadScheduledExecutor("ObsLogger task log setting renew Thread");
		scheduleExecutorService.scheduleAtFixedRate(this::renewTaskLogSetting, 0L, PERIOD_SECOND, TimeUnit.SECONDS);
		scheduleExecutorService.scheduleAtFixedRate(this::removeTaskLogger, LOGGER_REMOVE_WAIT_AFTER_MILLIS, LOGGER_REMOVE_WAIT_AFTER_MILLIS, TimeUnit.MILLISECONDS);
	}

	private static final long PERIOD_SECOND = 10L;
	private static final long LOGGER_REMOVE_WAIT_AFTER_MILLIS = 60000L;

	private final SettingService settingService;
	private final ClientMongoOperator clientMongoOperator;
	private final Map<String, TaskLogger> taskLoggersMap = new ConcurrentHashMap<>();
	private final Map<String, Long> loggerToBeRemoved = new ConcurrentHashMap<>();
	private final Map<String, Map<String, TaskLoggerNodeProxy>> taskLoggerNodeProxyMap = new ConcurrentHashMap<>();

	private final ScheduledExecutorService scheduleExecutorService;

	private void renewTaskLogSetting() {
		for (String taskId : taskLoggersMap.keySet()) {
			try {
				TaskDto task = clientMongoOperator.findOne(
						new Query(Criteria.where("_id").is(new ObjectId(taskId))), ConnectorConstant.TASK_COLLECTION, TaskDto.class
				);
				if (Objects.isNull(task)) {
					taskLoggersMap.remove(taskId);
					continue;
				}
				taskLoggersMap.computeIfPresent(taskId, (id, taskLogger) -> {
					taskLogger.withTaskLogSetting(getLogSettingLogLevel(task),
							getLogSettingRecordCeiling(task), getLogSettingIntervalCeiling(task));

					return taskLogger;
				});
			} catch (Throwable throwable) {
				logger.warn("failed to renew task logger setting for task {}: {}", taskId, throwable.getMessage(), throwable);
			}
		}
	}

	public ObsLogger getObsLogger(TaskDto task) {
		String taskId = task.getId().toHexString();
		taskLoggersMap.computeIfPresent(taskId, (k, v) -> v.withTask(taskId, task.getName(), task.getTaskRecordId()));
		taskLoggersMap.computeIfAbsent(taskId, k -> {
			loggerToBeRemoved.remove(taskId);
			TaskLogger taskLogger = new TaskLogger(this::closeDebugForTask).withTask(taskId, task.getName(), task.getTaskRecordId()).withTaskLogSetting(
					getLogSettingLogLevel(task), getLogSettingRecordCeiling(task), getLogSettingIntervalCeiling(task));
			taskLogger.registerTaskFileAppender(taskId);
			taskLogger.registerTaskTmAppender(taskId);
			return taskLogger;
		});

		return taskLoggersMap.get(taskId);
	}

	public ObsLogger getObsLogger(String taskId) {
		return taskLoggersMap.get(taskId);
	}

	public ObsLogger getObsLogger(TaskDto task, String nodeId, String nodeName) {
		TaskLogger taskLogger = (TaskLogger) getObsLogger(task);

		String taskId = task.getId().toHexString();
		taskLoggerNodeProxyMap.putIfAbsent(taskId, new ConcurrentHashMap<>());
		taskLoggerNodeProxyMap.get(taskId).putIfAbsent(nodeId,
				new TaskLoggerNodeProxy().withTaskLogger(taskLogger).withNode(nodeId, nodeName));

		return taskLoggerNodeProxyMap.get(taskId).get(nodeId);
	}

	public ObsLogger getObsLogger(String taskId, String nodeId) {
		ObsLogger obsLogger = null;
		if (null != taskLoggerNodeProxyMap.get(taskId) && null != taskLoggerNodeProxyMap.get(taskId).get(nodeId)) {
			obsLogger = taskLoggerNodeProxyMap.get(taskId).get(nodeId);
		}
		return obsLogger;
	}

	public void removeTaskLoggerMarkRemove(TaskDto task) {
		String taskId = task.getId().toHexString();
		loggerToBeRemoved.putIfAbsent(taskId, System.currentTimeMillis());
	}

	public void removeTaskLogger() {
		List<String> removed = new ArrayList<>();
		for (Map.Entry<String, Long> entry : loggerToBeRemoved.entrySet()) {
			if (System.currentTimeMillis() - entry.getValue() < LOGGER_REMOVE_WAIT_AFTER_MILLIS) continue;

			String taskId = entry.getKey();
			try {
				taskLoggerNodeProxyMap.remove(taskId);
				taskLoggersMap.computeIfPresent(taskId, (key, taskLogger) -> {
					taskLogger.unregisterTaskFileAppender(taskId);
					taskLogger.unregisterTaskTmAppender();

					return null;
				});
			} catch (Throwable throwable) {
				logger.warn("failed when remove the task logger or logger proxy for task node", throwable);
			} finally {
				taskLoggerNodeProxyMap.remove(taskId);
				taskLoggersMap.remove(taskId);
				removed.add(taskId);
			}
		}
		for (String taskId : removed) {
			loggerToBeRemoved.remove(taskId);
		}
	}

	public void closeDebugForTask(String taskId, LogLevel logLevel) {
		if (null == logLevel) {
			logLevel = LogLevel.INFO;
		}
		clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/logSetting/" + logLevel.getLevel(), taskId, TaskDto.class);
	}

	private static final String LOG_SETTING_LEVEL = "level";
	private static final String LOG_SETTING_RECORD_CEILING = "recordCeiling";
	private static final String LOG_SETTING_INTERVAL_CEILING = "intervalCeiling";

	private String getLogSettingLogLevel(TaskDto task) {
		if (null != task) {
			Map<String, Object> logSetting = task.getLogSetting();
			if (null != logSetting) {
				return (String) logSetting.get(LOG_SETTING_LEVEL);
			}
		}
		return settingService.getSetting("logLevel").getValue();
	}

	private Long getLogSettingRecordCeiling(TaskDto task) {
		if (null != task) {
			Map<String, Object> logSetting = task.getLogSetting();
			if (null != logSetting && null != logSetting.get(LOG_SETTING_RECORD_CEILING)) {
				return ((Integer) logSetting.get(LOG_SETTING_RECORD_CEILING)).longValue();
			}
		}

		return null;
	}

	private Long getLogSettingIntervalCeiling(TaskDto task) {
		if (null != task) {
			Map<String, Object> logSetting = task.getLogSetting();
			if (null != logSetting && null != logSetting.get(LOG_SETTING_INTERVAL_CEILING)) {
				return ((Integer) logSetting.get(LOG_SETTING_INTERVAL_CEILING)).longValue();
			}
		}
		return null;
	}
}