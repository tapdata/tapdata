package com.tapdata.processor.context;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author jackin
 * @date 2020/11/19 12:42 PM
 **/
public class ProcessContextEvent implements Serializable {

	private static final long serialVersionUID = -2583853079390066758L;
	private String op;

	private String tableName;

	private String syncType;

	private String type;

	private long ts;

	private Map<String, Object> updateFields;

	private List<String> removeFields;

	private Map<String, Object> before;

	public ProcessContextEvent(String op, String tableName, String syncType, long ts) {
		this.op = op;
		this.tableName = tableName;
		this.syncType = syncType;
		this.ts = ts;
	}

	public String getOp() {
		return op;
	}

	public void setOp(String op) {
		this.op = op;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getSyncType() {
		return syncType;
	}

	public void setSyncType(String syncType) {
		this.syncType = syncType;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	public Map<String, Object> getUpdateFields() {
		return updateFields;
	}

	public List<String> getRemoveFields() {
		return removeFields;
	}

	public void setRemoveFields(List<String> removeFields) {
		this.removeFields = removeFields;
	}

	public void setUpdateFields(Map<String, Object> updateFields) {
		this.updateFields = updateFields;
	}

	public Map<String, Object> getBefore() {
		return before;
	}

	public void setBefore(Map<String, Object> before) {
		this.before = before;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
}
