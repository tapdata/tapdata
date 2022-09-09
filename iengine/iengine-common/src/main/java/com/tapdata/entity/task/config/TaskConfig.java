package com.tapdata.entity.task.config;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.commons.collections.MapUtils;

import java.io.Serializable;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-09-03 14:28
 **/
public class TaskConfig implements Serializable {
	private static final long serialVersionUID = -240777904169993408L;

	public static TaskConfig create() {
		return new TaskConfig();
	}

	private TaskDto taskDto;

	public TaskConfig taskDto(TaskDto taskDto) {
		this.taskDto = taskDto;
		return this;
	}

	private TaskRetryConfig taskRetryConfig;

	public TaskConfig taskRetryConfig(TaskRetryConfig taskRetryConfig) {
		this.taskRetryConfig = taskRetryConfig;
		return this;
	}

	public TaskRetryConfig getTaskRetryConfig() {
		return taskRetryConfig;
	}

	public TaskDto getTaskDto() {
		return taskDto;
	}

	private Map<String, ExternalStorageDto> externalStorageDtoMap;

	public TaskConfig externalStorageDtoMap(Map<String, ExternalStorageDto> externalStorageDtoMap) {
		this.externalStorageDtoMap = externalStorageDtoMap;
		return this;
	}

	public Map<String, ExternalStorageDto> getExternalStorageDtoMap() {
		return externalStorageDtoMap;
	}

	public ExternalStorageDto getDefaultExternalStorage() {
		if (MapUtils.isEmpty(externalStorageDtoMap)) {
			return null;
		}
		ExternalStorageDto externalStorageDto = externalStorageDtoMap.values().stream().filter(ExternalStorageDto::isDefaultStorage).findFirst().orElse(null);
		if (null == externalStorageDto) {
			externalStorageDto = externalStorageDtoMap.values().stream().filter(e -> e.getName().equals(ConnectorConstant.TAPDATA_MONGO_DB_EXTERNAL_STORAGE_NAME)).findFirst().orElse(null);
		}
		return externalStorageDto;
	}
}
