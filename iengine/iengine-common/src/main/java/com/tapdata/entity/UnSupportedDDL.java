package com.tapdata.entity;

import java.io.Serializable;

/**
 * Created by xj
 * 2020-03-06 16:32
 **/
public class UnSupportedDDL implements Serializable {

	private static final long serialVersionUID = -2323577607737451376L;
	private Long scn;

	private String sql;

	private Long timestamp;

	private String xid;

	private String offset;

	/**
	 * 发送ddl的表
	 */
	private String tableName;

	public UnSupportedDDL() {
	}

	public UnSupportedDDL(Long scn, String sql, Long timestamp, String xid, String tableName) {
		this.scn = scn;
		this.sql = sql;
		this.timestamp = timestamp;
		this.xid = xid;
		this.tableName = tableName;
	}

	public UnSupportedDDL(String sql, Long timestamp, String offset) {
		this.sql = sql;
		this.timestamp = timestamp;
		this.offset = offset;
	}

	public UnSupportedDDL(String sql, Long timestamp, String offset, String tableName) {
		this.sql = sql;
		this.timestamp = timestamp;
		this.offset = offset;
		this.tableName = tableName;
	}

	public Long getScn() {
		return scn;
	}

	public void setScn(Long scn) {
		this.scn = scn;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public String getXid() {
		return xid;
	}

	public void setXid(String xid) {
		this.xid = xid;
	}

	public String getOffset() {
		return offset;
	}

	public void setOffset(String offset) {
		this.offset = offset;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
