package com.tapdata.entity.sharecdc;

import io.tapdata.annotation.Ignore;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author samuel
 * @Description
 * @create 2020-07-22 19:00
 **/
public class LogContent implements Serializable {

	@Ignore
	private static final long serialVersionUID = 5548333569381422473L;
	private String fromTable;
	private List<String> tableNamespaces;
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
	private byte[] tapDDLEvent;

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

	private LogContent(String fromTable, Long timestamp, Map<String, Object> before, Map<String, Object> after,
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

	public static LogContent createDMLLogContent(String fromTable, Long timestamp, Map<String, Object> before, Map<String, Object> after,
												 String op, String offsetString) {
		return new LogContent(fromTable, timestamp, before, after, op, offsetString);
	}

	private LogContent(String fromTable, Long timestamp, String op, String offsetString, byte[] tapDDLEvent) {
		this.fromTable = fromTable;
		this.timestamp = timestamp;
		this.op = op;
		this.offsetString = offsetString;
		this.tapDDLEvent = tapDDLEvent;
	}

	public static LogContent createDDLLogContent(String fromTable, Long timestamp, String op, String offsetString, byte[] tapDDLEvent) {
		return new LogContent(fromTable, timestamp, op, offsetString, tapDDLEvent);
	}

	public String getFromTable() {
		return fromTable;
	}

	public void setFromTable(String fromTable) {
		this.fromTable = fromTable;
	}

	public List<String> getTableNamespaces() {
		return tableNamespaces;
	}

	public void setTableNamespaces(List<String> tableNamespaces) {
		this.tableNamespaces = tableNamespaces;
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

	public void setTapDDLEvent(byte[] tapDDLEvent) {
		this.tapDDLEvent = tapDDLEvent;
	}

	public byte[] getTapDDLEvent() {
		return tapDDLEvent;
	}

	public static LogContent valueOf(Document document) {
		LogContent logContent = new LogContent();
		logContent.setFromTable(document.getOrDefault("fromTable", "").toString());
		Optional.ofNullable(document.getOrDefault("tableNamespaces", null)).map(tableNamespaces -> {
			if (tableNamespaces instanceof List) {
				logContent.setTableNamespaces((List<String>) tableNamespaces);
			}
			return null;
		});
		Object beforeObject = document.getOrDefault("before", null);
		if (beforeObject instanceof Map) {
			logContent.setBefore((Map<String, Object>) beforeObject);
		}
		Object afterObject = document.getOrDefault("after", null);
		if (afterObject instanceof Map) {
			logContent.setAfter((Map<String, Object>) afterObject);
		}
		logContent.setTimestamp(document.getLong("timestamp"));
		logContent.setOp(document.getOrDefault("op", "").toString());
		logContent.setOffsetString(document.getOrDefault("offsetString", "").toString());
		LogContentType logContentType = LogContentType.DATA;
		if (document.containsKey("type")) {
			String typeStr = document.getOrDefault("type", "").toString();
			if (StringUtils.isNotBlank(typeStr)) {
				try {
					logContentType = LogContentType.valueOf(typeStr);
				} catch (IllegalArgumentException ignored) {
				}
			}
		}
		Object tapDDLEventObj = document.get("tapDDLEvent");
		if (tapDDLEventObj instanceof byte[]) {
			logContent.setTapDDLEvent((byte[]) tapDDLEventObj);
		}
		logContent.setType(logContentType.name());
		return logContent;
	}

	@Override
	public String toString() {
		Object tapDDLEventObj = null;
		if (null != tapDDLEvent) {
			try {
				tapDDLEventObj = InstanceFactory.instance(ObjectSerializable.class).toObject(tapDDLEvent);
			} catch (Exception ignored) {
			}
		}
		return "LogContent{" +
				"\n  fromTable='" + fromTable + '\'' +
				"\n  tableNamespaces='" + tableNamespaces + '\'' +
				"\n  timestamp=" + new Date(timestamp).toInstant() +
				"\n  op=" + op +
				"\n  before=" + before +
				"\n  after=" + after +
				"\n  ddl event=" + (null != tapDDLEventObj ? tapDDLEventObj : "") +
				"\n  offsetString=" + offsetString +
				"\n  type=" + type +
				"\n}";
	}

	public enum LogContentType {
		DATA,
		SIGN,
	}
}
