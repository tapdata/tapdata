/**
 * @title: CdcLastTime
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

public class CdcLastTime {

	private String sourceConnectionId;

	private String sourceConnectionName;

	private String targetConnectionId;

	private String targetConnectionName;

	private Long cdcTime;

	public String getSourceConnectionId() {
		return sourceConnectionId;
	}

	public String getSourceConnectionName() {
		return sourceConnectionName;
	}

	public String getTargetConnectionId() {
		return targetConnectionId;
	}

	public String getTargetConnectionName() {
		return targetConnectionName;
	}

	public Long getCdcTime() {
		return cdcTime;
	}

	public void setSourceConnectionId(String sourceConnectionId) {
		this.sourceConnectionId = sourceConnectionId;
	}

	public void setSourceConnectionName(String sourceConnectionName) {
		this.sourceConnectionName = sourceConnectionName;
	}

	public void setTargetConnectionId(String targetConnectionId) {
		this.targetConnectionId = targetConnectionId;
	}

	public void setTargetConnectionName(String targetConnectionName) {
		this.targetConnectionName = targetConnectionName;
	}

	public void setCdcTime(Long cdcTime) {
		this.cdcTime = cdcTime;
	}
}
