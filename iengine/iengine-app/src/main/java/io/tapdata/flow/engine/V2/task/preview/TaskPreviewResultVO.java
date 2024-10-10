package io.tapdata.flow.engine.V2.task.preview;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.http.HttpStatus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author samuel
 * @Description
 * @create 2024-09-18 17:10
 **/
public class TaskPreviewResultVO {
	private String taskId;
	private int code;
	private final Map<String, TaskPreviewNodeResultVO> nodeResult = new ConcurrentHashMap<>();
	private String errorMsg;
	private String errorStack;
	private TaskPReviewStatsVO stats;

	private TaskPreviewResultVO() {
	}

	public TaskPreviewResultVO(TaskDto taskDto) {
		if (null != taskDto && null != taskDto.getId()) {
			this.taskId = taskDto.getId().toHexString();
		}
		this.code = HttpStatus.SC_OK;
	}

	public static TaskPreviewResultVO parseFailed(Exception e) {
		return new TaskPreviewResultVO().failed(e);
	}

	public TaskPreviewResultVO failed(Exception e) {
		this.code = HttpStatus.SC_INTERNAL_SERVER_ERROR;
		this.errorMsg = e.getMessage();
		this.errorStack = Log4jUtil.getStackString(e);
		return this;
	}

	public static TaskPreviewResultVO invalid(TaskDto taskDto, String message) {
		return new TaskPreviewResultVO(taskDto).invalid(message);
	}

	public TaskPreviewResultVO invalid(String message) {
		this.code = HttpStatus.SC_UNPROCESSABLE_ENTITY;
		this.errorMsg = message;
		return this;
	}

	public TaskPreviewNodeResultVO nodeResult(String nodeId) {
		return this.nodeResult.computeIfAbsent(nodeId, k -> new TaskPreviewNodeResultVO());
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getErrorMsg() {
		return errorMsg;
	}

	public String getErrorStack() {
		return errorStack;
	}

	public Map<String, TaskPreviewNodeResultVO> getNodeResult() {
		return nodeResult;
	}

	public String getTaskId() {
		return taskId;
	}

	public TaskPReviewStatsVO getStats() {
		return stats;
	}

	public void setStats(TaskPReviewStatsVO stats) {
		this.stats = stats;
	}
}
