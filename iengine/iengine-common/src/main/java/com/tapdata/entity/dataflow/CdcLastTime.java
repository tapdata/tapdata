/**
 * @title: CdcLastTime
 * @description:
 * @author lk
 * @date 2020/8/14
 */
package com.tapdata.entity.dataflow;

import java.io.Serializable;

public class CdcLastTime implements Serializable {

	private static final long serialVersionUID = 8749645828360602424L;

	private String sourceConnectionId;

	private String sourceConnectionName;

	private String targetConnectionId;

	private String targetConnectionName;

	private long cdcTime;

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

	public long getCdcTime() {
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

	public void setCdcTime(long cdcTime) {
		this.cdcTime = cdcTime;
	}
}
