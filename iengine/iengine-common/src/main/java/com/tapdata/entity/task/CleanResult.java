package com.tapdata.entity.task;

import java.io.Serializable;

public class CleanResult implements Serializable {

	private static final long serialVersionUID = -6330784701876221907L;

	private String deleteCondition;

	private String tableName;

	private String connection;

	private Long deletedRows;

	private Long totalRows;

	private String failedReason;

	public CleanResult() {
	}

	public CleanResult(String deleteCondition, String tableName, String connection, Long deletedRows, Long totalRows, String failedReason) {
		this.deleteCondition = deleteCondition;
		this.tableName = tableName;
		this.connection = connection;
		this.deletedRows = deletedRows;
		this.totalRows = totalRows;
		this.failedReason = failedReason;
	}

	public String getDeleteCondition() {
		return deleteCondition;
	}

	public void setDeleteCondition(String deleteCondition) {
		this.deleteCondition = deleteCondition;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getConnection() {
		return connection;
	}

	public void setConnection(String connection) {
		this.connection = connection;
	}

	public Long getDeletedRows() {
		return deletedRows;
	}

	public void setDeletedRows(Long deletedRows) {
		this.deletedRows = deletedRows;
	}

	public Long getTotalRows() {
		return totalRows;
	}

	public void setTotalRows(Long totalRows) {
		this.totalRows = totalRows;
	}

	public String getFailedReason() {
		return failedReason;
	}

	public void setFailedReason(String failedReason) {
		this.failedReason = failedReason;
	}
}
