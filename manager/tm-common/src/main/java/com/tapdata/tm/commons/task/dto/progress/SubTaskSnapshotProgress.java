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
public class SubTaskSnapshotProgress extends BaseDto implements Serializable {

	private static final long serialVersionUID = -8750390704591699906L;


	//共用属性
	private Long waitForRunNumber;
	// 值为-1的时候，为还没有确定
	private Long finishNumber;
	private Long startTs;
	private Long endTs;
	private String subTaskId;
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

	public static SubTaskSnapshotProgress getSnapshotSubTaskProgress(String subTaskId) {
		SubTaskSnapshotProgress subTaskSnapshotProgress = new SubTaskSnapshotProgress();
		subTaskSnapshotProgress.subTaskId = subTaskId;
		subTaskSnapshotProgress.totalTaleNum = 0;
		subTaskSnapshotProgress.completeTaleNum = 0;
		subTaskSnapshotProgress.startTs = System.currentTimeMillis();
		subTaskSnapshotProgress.waitForRunNumber = -1L;
		subTaskSnapshotProgress.finishNumber = 0L;
		subTaskSnapshotProgress.type = ProgressType.SUB_TASK_PROGRESS;
		return subTaskSnapshotProgress;
	}

	public static SubTaskSnapshotProgress getSnapshotEdgeProgress(
			String subTaskId, String srcNodeId, String tgtNodeId,
			String srcConnId, String tgtConnId,
			String srcTableName, String tgtTableName
	) {
		SubTaskSnapshotProgress subTaskSnapshotProgress = new SubTaskSnapshotProgress();
		subTaskSnapshotProgress.subTaskId = subTaskId;
		subTaskSnapshotProgress.srcNodeId = srcNodeId;
		subTaskSnapshotProgress.tgtNodeId = tgtNodeId;
		subTaskSnapshotProgress.srcConnId = srcConnId;
		subTaskSnapshotProgress.tgtConnId = tgtConnId;
		subTaskSnapshotProgress.srcTableName = srcTableName;
		subTaskSnapshotProgress.tgtTableName = tgtTableName;
		subTaskSnapshotProgress.startTs = System.currentTimeMillis();
		subTaskSnapshotProgress.waitForRunNumber = -1L;
		subTaskSnapshotProgress.finishNumber = 0L;
		subTaskSnapshotProgress.type = ProgressType.EDGE_PROGRESS;
		subTaskSnapshotProgress.status = ProgressStatus.waiting;
		return subTaskSnapshotProgress;
	}

	public enum ProgressType {
		EDGE_PROGRESS,
		SUB_TASK_PROGRESS,
		;
	}

	public enum ProgressStatus{
		waiting,
		running,
		done,
		;
	}
}
