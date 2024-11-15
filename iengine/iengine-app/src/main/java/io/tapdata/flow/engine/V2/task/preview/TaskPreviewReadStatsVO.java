package io.tapdata.flow.engine.V2.task.preview;

import io.tapdata.entity.utils.DataMap;

/**
 * @author samuel
 * @Description
 * @create 2024-10-14 18:29
 **/
public class TaskPreviewReadStatsVO {
	private String tableName;
	private long readTaken;
	private int limit;
	private DataMap match;
	private String method;
	private int rows;
	private long initTaken;
	private long closeTaken;

	public TaskPreviewReadStatsVO() {
	}

	public String getTableName() {
		return tableName;
	}

	public long getReadTaken() {
		return readTaken;
	}

	public int getLimit() {
		return limit;
	}

	public DataMap getMatch() {
		return match;
	}

	public String getMethod() {
		return method;
	}

	public int getRows() {
		return rows;
	}

	public long getInitTaken() {
		return initTaken;
	}

	public void setInitTaken(long initTaken) {
		this.initTaken = initTaken;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setReadTaken(long readTaken) {
		this.readTaken = readTaken;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public void setMatch(DataMap match) {
		this.match = match;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public void setRows(int rows) {
		this.rows = rows;
	}

	public long getCloseTaken() {
		return closeTaken;
	}

	public void setCloseTaken(long closeTaken) {
		this.closeTaken = closeTaken;
	}
}
