//package com.tapdata.tm.task.entity;
//
//import com.tapdata.tm.base.entity.BaseEntity;
//import lombok.Data;
//import lombok.EqualsAndHashCode;
//
//import java.io.Serializable;
//
///**
// * @author samuel
// * @Description
// * @create 2022-03-11 16:09
// **/
//@EqualsAndHashCode(callSuper = true)
//@Data
//public abstract class BaseSnapshotProgress extends BaseEntity implements Serializable {
//
//	private static final long serialVersionUID = -5485353207971912333L;
//
//	private Long waitForRunNumber;
//	private Long finishNumber;
//	private Long startTs;
//	private String subTaskId;
//	private com.tapdata.tm.commons.task.dto.progress.BaseSnapshotProgress.ProgressType type;
//
//	public BaseSnapshotProgress(String subTaskId, com.tapdata.tm.commons.task.dto.progress.BaseSnapshotProgress.ProgressType type) {
//		this.subTaskId = subTaskId;
//		this.waitForRunNumber = -1L;
//		this.finishNumber = 0L;
//		this.startTs = System.currentTimeMillis();
//		this.type = type;
//	}
//
//	public BaseSnapshotProgress(){}
//}
