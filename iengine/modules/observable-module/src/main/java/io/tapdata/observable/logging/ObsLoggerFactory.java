package io.tapdata.observable.logging;


import com.tapdata.tm.commons.task.dto.TaskDto;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Dexter
 **/
public final class ObsLoggerFactory {
	private volatile static ObsLoggerFactory INSTANCE;
	public static ObsLoggerFactory getInstance(){
		if (INSTANCE == null) {
			synchronized (ObsLoggerFactory.class) {
				if (INSTANCE == null) {
					INSTANCE = new ObsLoggerFactory();
				}
			}
		}
		return INSTANCE;
	}

	private ObsLoggerFactory() {}

	private final Map<String, TaskLogger> taskLoggersMap = new ConcurrentHashMap<>();
	private final Map<String, Map<String, TaskLoggerNodeProxy>> taskLoggerNodeProxyMap = new ConcurrentHashMap<>();

	public ObsLogger getObsLogger(TaskDto task) {
		String taskId = task.getId().toHexString();
		taskLoggersMap.computeIfPresent(taskId, (k, v) -> v.withTask(taskId, task.getName(), task.getTaskRecordId()));
		taskLoggersMap.putIfAbsent(taskId, new TaskLogger().withTask(taskId, task.getName(), task.getTaskRecordId()));
		taskLoggersMap.get(taskId).registerTaskFileAppender(taskId);

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
		return null;
	}

	public void removeTaskLogger(TaskDto task) {
		String taskId = task.getId().toHexString();
		taskLoggerNodeProxyMap.remove(taskId);
		taskLoggersMap.computeIfPresent(taskId, (key, taskLogger) -> {
			taskLogger.unregisterTaskFileAppender(taskId);

			return null;
		});
	}
}