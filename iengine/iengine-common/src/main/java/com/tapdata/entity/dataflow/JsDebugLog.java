package com.tapdata.entity.dataflow;

import java.io.Serializable;
import java.util.Date;

/**
 * js debug 日志
 *
 * @author jackin
 */
public class JsDebugLog implements Serializable {

	private static final long serialVersionUID = 4140791360724596758L;

	private String level;

	private String message;

	private String loggerName;

	private String threadName;

	private Date date;

	public JsDebugLog(
			String level,
			String message,
			Date createTime,
			String loggerName,
			String threadName
	) {
		this.level = level;
		this.message = message;
		this.loggerName = loggerName;
		this.threadName = threadName;
		this.threadName = threadName;
		this.date = createTime;
	}

	public JsDebugLog() {
	}

	public String getLevel() {
		return level;
	}

	public void setLevel(String level) {
		this.level = level;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getLoggerName() {
		return loggerName;
	}

	public void setLoggerName(String loggerName) {
		this.loggerName = loggerName;
	}

	public String getThreadName() {
		return threadName;
	}

	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}
}
