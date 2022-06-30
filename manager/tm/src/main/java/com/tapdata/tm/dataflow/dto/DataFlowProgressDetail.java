/**
 * @title: DataFlowProgressDetail
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

public class DataFlowProgressDetail {
	
	private String sourceConnectionId;
	
	private String sourceConnectionName;
	
	private String targetConnectionId;
	
	private String targetConnectionName;
	
	private String sourceTableName;
	
	private Long sourceRowNum;
	
	private String targetTableName;
	
	private Long targetRowNum;
	
	private DataFlowProgressStatus status;
	
	private Long cdcRowNum;
	
	private Long lag;
	
	private Long startTime;
	
	private String sourceDbName;
	
	private String targetDbName;

	private Integer sourceTableNum;
	
	private Integer targetTableNum;

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

	public String getSourceTableName() {
		return sourceTableName;
	}

	public Long getSourceRowNum() {
		return sourceRowNum;
	}

	public String getTargetTableName() {
		return targetTableName;
	}

	public Long getTargetRowNum() {
		return targetRowNum;
	}

	public DataFlowProgressStatus getStatus() {
		return status;
	}

	public Long getCdcRowNum() {
		return cdcRowNum;
	}

	public Long getLag() {
		return lag;
	}

	public Long getStartTime() {
		return startTime;
	}

	public String getSourceDbName() {
		return sourceDbName;
	}

	public String getTargetDbName() {
		return targetDbName;
	}

	public Integer getSourceTableNum() {
		return sourceTableNum;
	}

	public Integer getTargetTableNum() {
		return targetTableNum;
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

	public void setSourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
	}

	public void setSourceRowNum(Long sourceRowNum) {
		this.sourceRowNum = sourceRowNum;
	}

	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}

	public void setTargetRowNum(Long targetRowNum) {
		this.targetRowNum = targetRowNum;
	}

	public void setStatus(DataFlowProgressStatus status) {
		this.status = status;
	}

	public void setCdcRowNum(Long cdcRowNum) {
		this.cdcRowNum = cdcRowNum;
	}

	public void setLag(Long lag) {
		this.lag = lag;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}

	public void setSourceDbName(String sourceDbName) {
		this.sourceDbName = sourceDbName;
	}

	public void setTargetDbName(String targetDbName) {
		this.targetDbName = targetDbName;
	}

	public void setSourceTableNum(Integer sourceTableNum) {
		this.sourceTableNum = sourceTableNum;
	}

	public void setTargetTableNum(Integer targetTableNum) {
		this.targetTableNum = targetTableNum;
	}
}
