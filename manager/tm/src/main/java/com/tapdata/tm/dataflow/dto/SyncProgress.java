/**
 * @title: SyncProgress
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

public class SyncProgress {

	private String offset;

	private Long eventTime;

	private String syncStage;

	public String getOffset() {
		return offset;
	}

	public Long getEventTime() {
		return eventTime;
	}

	public String getSyncStage() {
		return syncStage;
	}

	public void setOffset(String offset) {
		this.offset = offset;
	}

	public void setEventTime(Long eventTime) {
		this.eventTime = eventTime;
	}

	public void setSyncStage(String syncStage) {
		this.syncStage = syncStage;
	}
}
