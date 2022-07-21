/**
 * @title: ValidatorStage
 * @description:
 * @author lk
 * @date 2020/7/8
 */
package com.tapdata.entity.dataflow.validator;

import java.io.Serializable;

public class ValidatorStage implements Serializable {

	private static final long serialVersionUID = -5906528577012124560L;

	private String stageId;

	private String tableName;

	private String filter;

	private String connectionId;

	private String databaseName;

	private String databaseType;

	public String getStageId() {
		return stageId;
	}

	public String getTableName() {
		return tableName;
	}

	public String getFilter() {
		return filter;
	}

	public void setStageId(String stageId) {
		this.stageId = stageId;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public String getConnectionId() {
		return connectionId;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getDatabaseType() {
		return databaseType;
	}

	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public void setDatabaseType(String databaseType) {
		this.databaseType = databaseType;
	}

}
