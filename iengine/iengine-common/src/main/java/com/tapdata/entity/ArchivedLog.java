package com.tapdata.entity;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author huangjq
 * @ClassName: ArchivedLog
 * @Description: 归档日志实体
 * @date 17-8-18 上午10:37
 * @since 1.0
 */
public class ArchivedLog {
	/**
	 * archived log location
	 */
	private String name;

	/**
	 * first_change scn
	 */
	private long firstChangeScn;

	/**
	 * next_change scn
	 */
	private long nextChangeScn;

	public ArchivedLog() {
	}

	public ArchivedLog(String name, long firstChangeScn, long nextChangeScn) {
		this.name = name;
		this.firstChangeScn = firstChangeScn;
		this.nextChangeScn = nextChangeScn;
	}

	public ArchivedLog(ResultSet resultSet) throws SQLException {
		this.name = resultSet.getString("name");
		this.firstChangeScn = resultSet.getLong("first_change#");
		this.nextChangeScn = resultSet.getLong("next_change#");
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getFirstChangeScn() {
		return firstChangeScn;
	}

	public void setFirstChangeScn(long firstChangeScn) {
		this.firstChangeScn = firstChangeScn;
	}

	public long getNextChangeScn() {
		return nextChangeScn;
	}

	public void setNextChangeScn(long nextChangeScn) {
		this.nextChangeScn = nextChangeScn;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("ArchivedLog{");
		sb.append("name='").append(name).append('\'');
		sb.append(", firstChangeScn=").append(firstChangeScn);
		sb.append('}');
		return sb.toString();
	}
}
