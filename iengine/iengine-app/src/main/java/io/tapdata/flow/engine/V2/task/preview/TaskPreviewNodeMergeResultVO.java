package io.tapdata.flow.engine.V2.task.preview;

import java.util.*;

/**
 * @author samuel
 * @Description
 * @create 2024-10-15 15:50
 **/
public class TaskPreviewNodeMergeResultVO extends TaskPreviewNodeResultVO {
	private static final String TAG = TaskPreviewNodeMergeResultVO.class.getName();
	private Map<String, Set<String>> fieldsMapping;

	private TaskPreviewNodeMergeResultVO() {
	}

	public static TaskPreviewNodeMergeResultVO create() {
		TaskPreviewNodeMergeResultVO taskPreviewNodeMergeResultVO = new TaskPreviewNodeMergeResultVO();
		taskPreviewNodeMergeResultVO.fieldsMapping = new LinkedHashMap<>();
		return taskPreviewNodeMergeResultVO;
	}

	public void addFieldMapping(String nodeId, String field) {
		this.fieldsMapping.computeIfAbsent(nodeId, k -> new HashSet<>()).add(field);
	}

	public Map<String, Set<String>> getFieldsMapping() {
		return fieldsMapping;
	}
}
