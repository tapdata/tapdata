package com.tapdata.entity.task.config;

import com.tapdata.entity.task.error.TaskServiceExCode_23;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.exception.TapCodeException;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author samuel
 * @Description
 * @create 2023-06-01 10:53
 **/
public enum TaskGlobalVariable implements MemoryFetcher {
	INSTANCE,
	;
	private final ConcurrentHashMap<String, Map<String, Object>> map = new ConcurrentHashMap<>();

	public Map<String, Object> getTaskGlobalVariable(String taskId) {
		if (StringUtils.isBlank(taskId)) {
			throw new TapCodeException(TaskServiceExCode_23.TASK_GLOBAL_VARIABLE_INIT_TASK_ID_EMPTY);
		}
		return map.computeIfAbsent(taskId, k -> new HashMap<>());
	}

	public void removeTask(String taskId) {
		if (StringUtils.isBlank(taskId)) {
			return;
		}
		map.remove(taskId);
	}

	public final static String SOURCE_INITIAL_COUNTER_KEY = "SOURCE_INITIAL_COUNTER";
	public final static String PREVIEW_COMPLETE_KEY = "PREVIEW_COMPLETE";

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = DataMap.create();
		for (Map.Entry<String, Map<String, Object>> entry : map.entrySet()) {
			String key = entry.getKey();
			Map<String, Object> value = entry.getValue();
			dataMap.kv(key, DataMap.create(value));
		}
		return dataMap;
	}
}
