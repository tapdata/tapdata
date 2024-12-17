package io.tapdata.flow.engine.V2.task.preview;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.preview.entity.PreviewConnectionInfo;
import io.tapdata.schema.TapTableMap;

import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-09-26 18:09
 **/
public class TaskPreviewInstance {
	private TaskDto taskDto;
	private TaskPreviewResultVO taskPreviewResultVO;
	private TaskClient<?> taskClient;
	private PreviewReadOperationQueue previewReadOperationQueue;
	private Map<String, PreviewConnectionInfo> nodeConnectionInfoMap = new HashMap<>();
	private Map<String, TapTableMap<String, TapTable>> tapTableMapHashMap;

	public TaskDto getTaskDto() {
		return taskDto;
	}

	public void setTaskDto(TaskDto taskDto) {
		this.taskDto = taskDto;
	}

	public TaskPreviewResultVO getTaskPreviewResultVO() {
		return taskPreviewResultVO;
	}

	public void setTaskPreviewResultVO(TaskPreviewResultVO taskPreviewResultVO) {
		this.taskPreviewResultVO = taskPreviewResultVO;
	}

	public TaskClient<?> getTaskClient() {
		return taskClient;
	}

	public void setTaskClient(TaskClient<?> taskClient) {
		this.taskClient = taskClient;
	}

	public PreviewReadOperationQueue getPreviewReadOperationQueue() {
		return previewReadOperationQueue;
	}

	public void setPreviewReadOperationQueue(PreviewReadOperationQueue previewReadOperationQueue) {
		this.previewReadOperationQueue = previewReadOperationQueue;
	}

	public Map<String, PreviewConnectionInfo> getNodeConnectionInfoMap() {
		return nodeConnectionInfoMap;
	}

	public void setNodeConnectionInfoMap(Map<String, PreviewConnectionInfo> nodeConnectionInfoMap) {
		this.nodeConnectionInfoMap = nodeConnectionInfoMap;
	}

	public Map<String, TapTableMap<String, TapTable>> getTapTableMapHashMap() {
		return tapTableMapHashMap;
	}

	public void setTapTableMapHashMap(Map<String, TapTableMap<String, TapTable>> tapTableMapHashMap) {
		this.tapTableMapHashMap = tapTableMapHashMap;
	}
}
