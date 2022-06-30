package com.tapdata.entity.dataflow;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2021-07-07 15:35
 **/
public class DataFlowProgressDetail implements Serializable {

	private static final long serialVersionUID = -7299091399045344732L;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String sourceConnectionId;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String sourceConnectionName;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String targetConnectionId;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String targetConnectionName;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String sourceTableName;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Long sourceRowNum;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String targetTableName;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Long targetRowNum;
	/**
	 * waiting, running, done
	 */
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Status status;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Long cdcRowNum;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Long lag;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Long startTime;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String sourceDbName;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String targetDbName;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Integer sourceTableNum;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Integer targetTableNum;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String sourceDatabaseType;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String targetDatabaseType;

	public DataFlowProgressDetail() {
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

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public Long getStartTime() {
		return startTime;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}

	public Long getCdcRowNum() {
		return cdcRowNum;
	}

	public void setCdcRowNum(Long cdcRowNum) {
		this.cdcRowNum = cdcRowNum;
	}

	public Long getLag() {
		return lag;
	}

	public void setLag(Long lag) {
		this.lag = lag;
	}

	public String getSourceDbName() {
		return sourceDbName;
	}

	public void setSourceDbName(String sourceDbName) {
		this.sourceDbName = sourceDbName;
	}

	public String getTargetDbName() {
		return targetDbName;
	}

	public void setTargetDbName(String targetDbName) {
		this.targetDbName = targetDbName;
	}

	public Integer getSourceTableNum() {
		return sourceTableNum;
	}

	public void setSourceTableNum(Integer sourceTableNum) {
		this.sourceTableNum = sourceTableNum;
	}

	public Integer getTargetTableNum() {
		return targetTableNum;
	}

	public void setTargetTableNum(Integer targetTableNum) {
		this.targetTableNum = targetTableNum;
	}

	public String getSourceDatabaseType() {
		return sourceDatabaseType;
	}

	public void setSourceDatabaseType(String sourceDatabaseType) {
		this.sourceDatabaseType = sourceDatabaseType;
	}

	public String getTargetDatabaseType() {
		return targetDatabaseType;
	}

	public void setTargetDatabaseType(String targetDatabaseType) {
		this.targetDatabaseType = targetDatabaseType;
	}

	public enum Status {
		waiting,
		running,
		done,
		;
	}

	@Override
	public String toString() {
		return "DataFlowProgressDetail{" +
				"sourceConnectionId='" + sourceConnectionId + '\'' +
				", sourceConnectionName='" + sourceConnectionName + '\'' +
				", targetConnectionId='" + targetConnectionId + '\'' +
				", targetConnectionName='" + targetConnectionName + '\'' +
				", sourceTableName='" + sourceTableName + '\'' +
				", sourceRowNum=" + sourceRowNum +
				", targetTableName='" + targetTableName + '\'' +
				", targetRowNum=" + targetRowNum +
				", status=" + status +
				", cdcRowNum=" + cdcRowNum +
				", lag=" + lag +
				", startTime=" + startTime +
				", sourceDbName='" + sourceDbName + '\'' +
				", targetDbName='" + targetDbName + '\'' +
				'}';
	}
}
