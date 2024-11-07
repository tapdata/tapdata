package io.tapdata.flow.engine.V2.task.preview;

import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-09-24 15:19
 **/
public class TaskPreviewMergeReadEntity {
	private String sourceNodeId;
	private List<Map<String, Object>> data;

	public TaskPreviewMergeReadEntity(String sourceNodeId) {
		this.sourceNodeId = sourceNodeId;
	}

	public String getSourceNodeId() {
		return sourceNodeId;
	}

	public List<Map<String, Object>> getData() {
		return data;
	}
}
