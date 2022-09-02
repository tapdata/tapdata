package com.tapdata.entity.task.config;

import com.tapdata.tm.commons.task.dto.TaskDto;

/**
 * @author samuel
 * @Description
 * @create 2022-09-03 14:28
 **/
public class TaskConfig {
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
}
