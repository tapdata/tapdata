package com.tapdata.tm.task.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.task.dto.progress.TaskSnapshotProgress;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2022-03-11 16:47
 **/
@Document("TaskProgress")
@Data
@EqualsAndHashCode(callSuper=false)
@AllArgsConstructor
@NoArgsConstructor
public class SnapshotEdgeProgressEntity extends BaseEntity implements Serializable {

	//共用属性
	private Long waitForRunNumber;
	private Long finishNumber;
	private Long startTs;
	private Long endTs;
	private String subTaskId;
	private TaskSnapshotProgress.ProgressType type;
	private String srcNodeId;
	private String tgtNodeId;
	private String srcConnId;
	private String tgtConnId;
	private TaskSnapshotProgress.ProgressStatus status;

	//概览独有属性
	private Integer totalTaleNum;
	private Integer completeTaleNum;


	//表级独有属性
	private String srcTableName;
	private String tgtTableName;

	//错误
	private String errorMsg;
}
