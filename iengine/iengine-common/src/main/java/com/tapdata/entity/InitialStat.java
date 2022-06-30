package com.tapdata.entity;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.tapdata.entity.dataflow.DataFlowProgressDetail;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2021-07-10 17:03
 **/
public class InitialStat implements Serializable {

	private static final long serialVersionUID = -8245032559193122969L;

	private String sourceConnectionId;
	private String sourceConnectionName;
	private String targetConnectionId;
	private String targetConnectionName;
	private String sourceTableName;
	private Long sourceRowNum;
	private String targetTableName;
	private volatile Long targetRowNum;
	private Long startTime;
	private DataFlowProgressDetail.Status status;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String sourceDbName;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String targetDbName;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String sourceDatabaseType;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String targetDatabaseType;

	public InitialStat() {
	}

	public InitialStat(String sourceConnectionId, String sourceConnectionName, String targetConnectionId, String targetConnectionName, String sourceTableName, String targetTableName, String sourceDbName, String targetDbName,
					   String sourceDatabaseType, String targetDatabaseType) {
		this.sourceConnectionId = sourceConnectionId;
		this.sourceConnectionName = sourceConnectionName;
		this.targetConnectionId = targetConnectionId;
		this.targetConnectionName = targetConnectionName;
		this.sourceTableName = sourceTableName;
		this.targetTableName = targetTableName;
		this.sourceRowNum = -1L;
		this.targetRowNum = 0L;
		this.startTime = 0L;
		this.status = DataFlowProgressDetail.Status.waiting;
		this.sourceDbName = sourceDbName;
		this.targetDbName = targetDbName;
		this.sourceDatabaseType = sourceDatabaseType;
		this.targetDatabaseType = targetDatabaseType;
	}

	public String getSourceConnectionId() {
		return sourceConnectionId;
	}

	public void setSourceConnectionId(String sourceConnectionId) {
		this.sourceConnectionId = sourceConnectionId;
	}

	public String getSourceConnectionName() {
		return sourceConnectionName;
	}

	public void setSourceConnectionName(String sourceConnectionName) {
		this.sourceConnectionName = sourceConnectionName;
	}

	public String getTargetConnectionId() {
		return targetConnectionId;
	}

	public void setTargetConnectionId(String targetConnectionId) {
		this.targetConnectionId = targetConnectionId;
	}

	public String getTargetConnectionName() {
		return targetConnectionName;
	}

	public void setTargetConnectionName(String targetConnectionName) {
		this.targetConnectionName = targetConnectionName;
	}

	public String getSourceTableName() {
		return sourceTableName;
	}

	public void setSourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
	}

	public Long getSourceRowNum() {
		return sourceRowNum;
	}

	public void setSourceRowNum(Long sourceRowNum) {
		this.sourceRowNum = sourceRowNum;
	}

	public String getTargetTableName() {
		return targetTableName;
	}

	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}

	public Long getTargetRowNum() {
		return targetRowNum;
	}

	public void setTargetRowNum(Long targetRowNum) {
		this.targetRowNum = targetRowNum;
	}

	public Long getStartTime() {
		return startTime;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}

	public DataFlowProgressDetail.Status getStatus() {
		return status;
	}

	public void setStatus(DataFlowProgressDetail.Status status) {
		this.status = status;
	}

	public String getSourceDbName() {
		return sourceDbName;
	}

	public String getTargetDbName() {
		return targetDbName;
	}

	public String getSourceDatabaseType() {
		return sourceDatabaseType;
	}

	public String getTargetDatabaseType() {
		return targetDatabaseType;
	}

	@Override
	public String toString() {
		return "InitialStat{" +
				"sourceConnectionId='" + sourceConnectionId + '\'' +
				", sourceConnectionName='" + sourceConnectionName + '\'' +
				", targetConnectionId='" + targetConnectionId + '\'' +
				", targetConnectionName='" + targetConnectionName + '\'' +
				", sourceTableName='" + sourceTableName + '\'' +
				", sourceRowNum=" + sourceRowNum +
				", targetTableName='" + targetTableName + '\'' +
				", targetRowNum=" + targetRowNum +
				", startTime=" + startTime +
				", status=" + status +
				", sourceDbName='" + sourceDbName + '\'' +
				", targetDbName='" + targetDbName + '\'' +
				'}';
	}
}
