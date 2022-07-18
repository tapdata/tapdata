package com.tapdata.entity.sharecdc;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2020-07-27 13:03
 **/
public class LogCollectOffset implements Serializable {

	private static final long serialVersionUID = 5868105848768696296L;
	private long timestamp;
	private Object collectorOffset;
	private String threadName;

	public LogCollectOffset() {

	}

	public LogCollectOffset(long timestamp, Object collectorOffset) {
		this.timestamp = timestamp;
		this.collectorOffset = collectorOffset;
	}

	public LogCollectOffset(long timestamp, Object collectorOffset, String threadName) {
		this.timestamp = timestamp;
		this.collectorOffset = collectorOffset;
		this.threadName = threadName;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public Object getCollectorOffset() {
		return collectorOffset;
	}

	public String getThreadName() {
		return threadName;
	}

	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}
}
