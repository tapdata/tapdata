package com.tapdata.tm.commons.metrics;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2021-12-07 19:53
 **/
@EqualsAndHashCode(callSuper = true)
@Data
public class TaskMetricsLabel extends MetricsLabel implements Serializable {

	private static final long serialVersionUID = 8057845021303839670L;

	private String taskId;
	private String nodeId;

	private TaskMetricsLabel() {
	}

	public TaskMetricsLabel(String taskId) {
		this.taskId = taskId;
	}

	public TaskMetricsLabel(String taskId, String nodeId) {
		this.taskId = taskId;
		this.nodeId = nodeId;
	}
}
