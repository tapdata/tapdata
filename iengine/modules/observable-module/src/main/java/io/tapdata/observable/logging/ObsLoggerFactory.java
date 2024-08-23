package io.tapdata.observable.logging;


import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.SettingService;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.exception.TmUnavailableException;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Dexter
 **/
@Slf4j
public final class ObsLoggerFactory implements MemoryFetcher {
	private final Logger logger = LogManager.getLogger(ObsLoggerFactory.class);

	private volatile static ObsLoggerFactory INSTANCE;

	public static ObsLoggerFactory getInstance() {
		if (INSTANCE == null) {
			synchronized (ObsLoggerFactory.class) {
				if (INSTANCE == null) {
					synchronized (ObsLoggerFactory.class) {
						INSTANCE = new ObsLoggerFactory();
					}
				}
			}
		}
		return INSTANCE;
	}

	private static final AtomicBoolean initialized = new AtomicBoolean(false);
	private ObsLoggerFactory() {
		this.settingService = Optional.ofNullable(BeanUtil.getBean(SettingService.class)).orElse((SettingService) getBeanAsync(SettingService.class));
		this.clientMongoOperator = Optional.ofNullable(BeanUtil.getBean(ClientMongoOperator.class)).orElse((ClientMongoOperator) getBeanAsync(ClientMongoOperator.class));
		this.scheduleExecutorService = new ScheduledThreadPoolExecutor(1);
		scheduleExecutorService.scheduleAtFixedRate(this::renewTaskLogSetting, 0L, PERIOD_SECOND, TimeUnit.SECONDS);
		scheduleExecutorService.scheduleWithFixedDelay(this::removeTaskLogger, PERIOD_SECOND, PERIOD_SECOND, TimeUnit.SECONDS);
	}

	Object getBeanAsync(Class<?> clz){
		while (!initialized.get()) {
			synchronized (initialized) {
				try {
					initialized.wait(100L);
					Object bean = BeanUtil.getBean(clz);
					if (null != bean ){
						return bean;
					}
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return null;
	}

	private static final long PERIOD_SECOND = 10L;
	private static final long LOGGER_REMOVE_WAIT_AFTER_MILLIS = TimeUnit.HOURS.toMillis(1L);
	private final SettingService settingService;
	private final ClientMongoOperator clientMongoOperator;
	private final Map<String, TaskLogger> taskLoggersMap = new ConcurrentHashMap<>();
	private final Map<String, Long> loggerToBeRemoved = new ConcurrentHashMap<>();
	private final Map<String, Map<String, TaskLoggerNodeProxy>> taskLoggerNodeProxyMap = new ConcurrentHashMap<>();

	private final ScheduledExecutorService scheduleExecutorService;
	public Map<String, TaskLogger> getTaskLoggersMap() {
		return taskLoggersMap;
	}
	private void renewTaskLogSetting() {
		Thread.currentThread().setName("Renew-Task-Logger-Setting-Scheduler");
		for (String taskId : taskLoggersMap.keySet()) {
			TaskLogger logger = taskLoggersMap.get(taskId);
			if (null == logger || logger.isTestTask()) continue;
			try {
				TaskDto task = clientMongoOperator.findOne(
						new Query(Criteria.where("_id").is(new ObjectId(taskId))), ConnectorConstant.TASK_COLLECTION, TaskDto.class
				);
				if (Objects.isNull(task)) continue;

				taskLoggersMap.computeIfPresent(taskId, (id, taskLogger) -> {
					taskLogger.withTaskLogSetting(getLogSettingLogLevel(task),
							getLogSettingRecordCeiling(task), getLogSettingIntervalCeiling(task));

					return taskLogger;
				});
			} catch (Throwable throwable) {
				if (TmUnavailableException.isInstance(throwable)) {
					this.logger.warn("Failed to renew task logger setting for task {}: TM unavailable: {}", taskId, throwable.getMessage());
				} else {
					this.logger.warn("Failed to renew task logger setting for task {}: {}", taskId, throwable.getMessage(), throwable);
				}
			}
		}
	}

	public ObsLogger getObsLogger(TaskDto task) {
		String taskId = task.getId().toHexString();
		taskLoggersMap.computeIfPresent(taskId, (k, v) -> v.withTask(taskId, task.getName(), task.getTaskRecordId()));
		taskLoggersMap.computeIfAbsent(taskId, k -> {
			loggerToBeRemoved.remove(taskId);
			TaskLogger taskLogger = TaskLogger.create(task, this::closeDebugForTask)
					.withTaskLogSetting(getLogSettingLogLevel(task), getLogSettingRecordCeiling(task), getLogSettingIntervalCeiling(task));
			taskLogger.start();
			return taskLogger;
		});

		return taskLoggersMap.get(taskId);
	}

	public boolean inFactory(String taskId) {
		return null != taskLoggersMap && !taskLoggersMap.isEmpty() && taskLoggersMap.containsKey(taskId);
	}

	public void removeFromFactory(String taskId){
		try {
			taskLoggerNodeProxyMap.remove(taskId);
			taskLoggersMap.computeIfPresent(taskId, (key, taskLogger) -> {
				try {
					logger.info("Remove task logger, task id: {}", key);
					taskLogger.close();
				} catch (Exception e) {
					throw new RuntimeException(String.format("Close task %s[%s] logger failed, error message: %s", taskLogger.getTaskName(), taskLogger.getTaskId(), e.getMessage()), e);
				}
				return null;
			});
			loggerToBeRemoved.remove(taskId);
			taskLoggersMap.remove(taskId);
		} catch (Throwable throwable) {
			logger.error("Failed when remove the task logger or logger proxy for task node", throwable);
		}
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
		logger.info("Add mark with call remove task logger, task id: {}", taskId);
		loggerToBeRemoved.putIfAbsent(taskId, System.currentTimeMillis());
	}

	public void removeTaskLoggerClearMark(TaskDto task) {
		synchronized (loggerToBeRemoved) {
			String taskId = task.getId().toHexString();
			loggerToBeRemoved.computeIfPresent(taskId, (s, markTimes) -> {
				logger.info("Clear mark with start task, task id: {}", taskId);
				return null;
			});
		}
	}

	public void removeTaskLogger(TaskDto task) {
		String taskId = task.getId().toHexString();
		removeTaskLoggerClearMark(task);
		synchronized (taskLoggersMap) {
			taskLoggersMap.computeIfPresent(taskId, (s, markTimes) -> {
				logger.info("Clear mark with start task, task id: {}", taskId);
				return null;
			});
		}
	}

	public void removeTaskLogger() {
		Thread.currentThread().setName("Remove-Task-Logger-Scheduler");
		synchronized (loggerToBeRemoved) {
			try {
				for (String taskId : loggerToBeRemoved.keySet()) {
					Long timestamp = loggerToBeRemoved.get(taskId);
					if (System.currentTimeMillis() - timestamp < LOGGER_REMOVE_WAIT_AFTER_MILLIS) continue;
					removeFromFactory(taskId);
				}
			} catch (Throwable e) {
				logger.error("Failed to remove task logger", e);
			}
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
	public LogConfiguration getLogConfiguration(String logConfigPrefix) {
		int logFileSaveTime = settingService.getInt(logConfigPrefix + "_log_file_save_time", 180);
		int logFileSaveSize = settingService.getInt(logConfigPrefix + "_log_file_save_size", 1024);
		int logFileSaveCount = settingService.getInt(logConfigPrefix + "_log_file_save_count", 100);
		LogConfiguration logConfiguration = LogConfiguration.builder().logSaveTime(logFileSaveTime)
				.logSaveSize(logFileSaveSize)
				.logSaveCount(logFileSaveCount)
				.build();
		return logConfiguration;
	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		return null;
	}
}
