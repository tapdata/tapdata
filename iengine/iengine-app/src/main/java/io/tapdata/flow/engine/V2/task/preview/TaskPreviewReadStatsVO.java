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

	public TaskPreviewReadStatsVO(String tableName, long readTaken, int limit, DataMap match, String method, int rows) {
		this.tableName = tableName;
		this.readTaken = readTaken;
		this.limit = limit;
		this.match = match;
		this.method = method;
		this.rows = rows;
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
}
