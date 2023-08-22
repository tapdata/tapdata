package com.tapdata.tm.monitor.param;

import lombok.Data;

import java.io.Serializable;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/22 10:59 Create
 */
@Data
public class SyncStatusStatisticsParam implements Serializable {
	private String taskId;
	private String taskRecordId;

	public SyncStatusStatisticsParam() {
	}

	public SyncStatusStatisticsParam(String taskId, String taskRecordId) {
		this.taskId = taskId;
		this.taskRecordId = taskRecordId;
	}
}
