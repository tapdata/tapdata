package com.tapdata.tm.commons.task.dto.progress;


import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.*;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2022-03-11 16:47
 **/
@EqualsAndHashCode(callSuper = true)
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class TaskSnapshotProgress extends BaseDto implements Serializable {

	private static final long serialVersionUID = -8750390704591699906L;


	//共用属性
	private Long waitForRunNumber;
	// 值为-1的时候，为还没有确定
	private Long finishNumber;
	private Long startTs;
	private Long endTs;
	private String taskId;
	private ProgressType type;
	private String srcNodeId;
	private String tgtNodeId;
	private String srcConnId;
	private String tgtConnId;
	private ProgressStatus status;

	//概览独有属性
	private Integer totalTaleNum;
	private Integer completeTaleNum;


	//表级独有属性
	private String srcTableName;
	private String tgtTableName;

	//错误
	private String errorMsg;

	public static TaskSnapshotProgress getSnapshotSubTaskProgress(String taskId) {
		TaskSnapshotProgress taskSnapshotProgress = new TaskSnapshotProgress();
		taskSnapshotProgress.taskId = taskId;
		taskSnapshotProgress.totalTaleNum = 0;
		taskSnapshotProgress.completeTaleNum = 0;
		taskSnapshotProgress.startTs = System.currentTimeMillis();
		taskSnapshotProgress.waitForRunNumber = -1L;
		taskSnapshotProgress.finishNumber = 0L;
		taskSnapshotProgress.type = ProgressType.TASK_PROGRESS;
		return taskSnapshotProgress;
	}

	public static TaskSnapshotProgress getSnapshotEdgeProgress(
			String taskId, String srcNodeId, String tgtNodeId,
			String srcConnId, String tgtConnId,
			String srcTableName, String tgtTableName
	) {
		TaskSnapshotProgress taskSnapshotProgress = new TaskSnapshotProgress();
		taskSnapshotProgress.taskId = taskId;
		taskSnapshotProgress.srcNodeId = srcNodeId;
		taskSnapshotProgress.tgtNodeId = tgtNodeId;
		taskSnapshotProgress.srcConnId = srcConnId;
		taskSnapshotProgress.tgtConnId = tgtConnId;
		taskSnapshotProgress.srcTableName = srcTableName;
		taskSnapshotProgress.tgtTableName = tgtTableName;
		taskSnapshotProgress.startTs = System.currentTimeMillis();
		taskSnapshotProgress.waitForRunNumber = -1L;
		taskSnapshotProgress.finishNumber = 0L;
		taskSnapshotProgress.type = ProgressType.EDGE_PROGRESS;
		taskSnapshotProgress.status = ProgressStatus.waiting;
		return taskSnapshotProgress;
	}

	public enum ProgressType {
		EDGE_PROGRESS,
		TASK_PROGRESS,
		;
	}

	public enum ProgressStatus{
		waiting,
		running,
		done,
		;
	}
}
