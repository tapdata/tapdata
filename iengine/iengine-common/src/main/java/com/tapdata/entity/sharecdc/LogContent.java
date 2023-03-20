package com.tapdata.entity.sharecdc;

import io.tapdata.annotation.Ignore;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2020-07-22 19:00
 **/
public class LogContent implements Serializable {

	@Ignore
	private static final long serialVersionUID = 5548333569381422473L;
	private String fromTable;
	@Deprecated
	private Map<String, Object> data;
	@Deprecated
	private Map<String, LogCollectOffset> offset;
	@Deprecated
	private String connStr;

	private Long timestamp;
	private Date date;
	private Map<String, Object> before;
	private Map<String, Object> after;
	private String op;
	private String offsetString;
	private String type = LogContentType.DATA.name();

	public LogContent() {
	}

	public static LogContent createStartTimeSign() {
		LogContent logContent = new LogContent();
		logContent.setTimestamp(System.currentTimeMillis());
		logContent.setType(LogContentType.SIGN.name());
		return logContent;
	}

	public LogContent(String fromTable, Map<String, Object> data, Map<String, LogCollectOffset> offset, String connStr, Long timestamp) {
		this.fromTable = fromTable;
		this.data = data;
		this.offset = offset;
		this.connStr = connStr;
		this.timestamp = timestamp;
	}

	public LogContent(String fromTable, Long timestamp, Map<String, Object> before, Map<String, Object> after,
					  String op, String offsetString) {
		this.fromTable = fromTable;
		this.timestamp = timestamp;
		this.before = before;
		this.after = after;
		this.op = op;
		this.offsetString = offsetString;
		if (timestamp != null) {
			this.date = new Date(timestamp);
		}
	}

	public String getFromTable() {
		return fromTable;
	}

	public void setFromTable(String fromTable) {
		this.fromTable = fromTable;
	}

	@Deprecated
	public Map<String, Object> getData() {
		return data;
	}

	@Deprecated
	public void setData(Map<String, Object> data) {
		this.data = data;
	}

	@Deprecated
	public Map<String, LogCollectOffset> getOffset() {
		return offset;
	}

	@Deprecated
	public void setOffset(Map<String, LogCollectOffset> offset) {
		this.offset = offset;
	}

	@Deprecated
	public String getConnStr() {
		return connStr;
	}

	@Deprecated
	public void setConnStr(String connStr) {
		this.connStr = connStr;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public Map<String, Object> getBefore() {
		return before;
	}

	public Map<String, Object> getAfter() {
		return after;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	@Deprecated
	public void setDate(Date date) {
		this.date = date;
	}

	public void setBefore(Map<String, Object> before) {
		this.before = before;
	}

	public void setAfter(Map<String, Object> after) {
		this.after = after;
	}

	public void setOp(String op) {
		this.op = op;
	}

	public void setOffsetString(String offsetString) {
		this.offsetString = offsetString;
	}

	public String getOp() {
		return op;
	}

	public String getOffsetString() {
		return offsetString;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public boolean isEmpty() {
		return StringUtils.isAnyBlank(fromTable, connStr)
				|| MapUtils.isEmpty(data)
				|| timestamp <= 0L;
	}

	public static LogContent valueOf(Document document) {
		LogContent logContent = new LogContent();
		logContent.setFromTable(document.getString("fromTable"));
		Object beforeObject = document.getOrDefault("before", null);
		if (beforeObject instanceof Map) {
			logContent.setBefore((Map<String, Object>) beforeObject);
		}
		Object afterObject = document.getOrDefault("after", null);
		if (afterObject instanceof Map) {
			logContent.setAfter((Map<String, Object>) afterObject);
		}
		logContent.setTimestamp(document.getLong("timestamp"));
		logContent.setOp(document.getString("op"));
		logContent.setOffsetString(document.getString("offsetString"));
		logContent.setType(LogContentType.valueOf(document.getString("type")).name());
		return logContent;
	}

	@Override
	public String toString() {
		return "LogContent{\n" +
				"  fromTable='" + fromTable +
				"\n  timestamp=" + new Date(timestamp).toInstant() +
				"\n  op=" + op +
				"\n  before=" + before +
				"\n  after=" + after +
				"\n  offsetString=" + offsetString +
				"\n  type=" + type +
				"\n}";
	}

	public enum LogContentType {
		DATA,
		SIGN,
	}
}
