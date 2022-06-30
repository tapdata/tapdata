/**
 * @title: ValidateStats
 * @description:
 * @author lk
 * @date 2020/7/9
 */
package com.tapdata.entity.dataflow.validator;

import java.io.Serializable;

public class ValidateStats implements Serializable {

	private static final long serialVersionUID = -7190389888635948871L;

	private String sourceDatabaseName;

	private String sourceTableName;

	private String targetDatabaseName;

	private String targetTableName;

	private String validateType;

	private long rows;

	private long rowsDiffer;

	private long rowsMismatch;

	private double consistencyRate;

	public String getSourceTableName() {
		return sourceTableName;
	}

	public String getValidateType() {
		return validateType;
	}

	public long getRowsDiffer() {
		return rowsDiffer;
	}

	public long getRowsMismatch() {
		return rowsMismatch;
	}

	public double getConsistencyRate() {
		return consistencyRate;
	}

	public void setSourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
	}

	public void setValidateType(String validateType) {
		this.validateType = validateType;
	}

	public void setRowsDiffer(long rowsDiffer) {
		this.rowsDiffer = rowsDiffer;
	}

	public void setRowsMismatch(long rowsMismatch) {
		this.rowsMismatch = rowsMismatch;
	}

	public void setConsistencyRate(double consistencyRate) {
		this.consistencyRate = consistencyRate;
	}

	public long getRows() {
		return rows;
	}

	public void setRows(long rows) {
		this.rows = rows;
	}

	public String getTargetTableName() {
		return targetTableName;
	}

	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}

	public String getSourceDatabaseName() {
		return sourceDatabaseName;
	}

	public String getTargetDatabaseName() {
		return targetDatabaseName;
	}

	public void setSourceDatabaseName(String sourceDatabaseName) {
		this.sourceDatabaseName = sourceDatabaseName;
	}

	public void setTargetDatabaseName(String targetDatabaseName) {
		this.targetDatabaseName = targetDatabaseName;
	}
}
