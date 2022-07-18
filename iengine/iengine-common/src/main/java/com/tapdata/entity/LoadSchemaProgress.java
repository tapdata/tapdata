package com.tapdata.entity;

/**
 * @author samuel
 * @Description
 * @create 2020-09-10 12:20
 **/
public class LoadSchemaProgress {
	private String connId;
	private int tableCount;
	private int loadCount;

	public LoadSchemaProgress() {

	}

	public LoadSchemaProgress(String connId, int tableCount, int loadCount) {
		this.connId = connId;
		this.tableCount = tableCount;
		this.loadCount = loadCount;
	}

	public String getConnId() {
		return connId;
	}

	public void setConnId(String connId) {
		this.connId = connId;
	}

	public int getTableCount() {
		return tableCount;
	}

	public void setTableCount(int tableCount) {
		this.tableCount = tableCount;
	}

	public int getLoadCount() {
		return loadCount;
	}

	public void increaLoadCount(int i) {
		this.loadCount += i;
	}
}
