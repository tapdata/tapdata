package io.tapdata.flow.engine.V2.task.preview;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.task.TaskClient;

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
}
