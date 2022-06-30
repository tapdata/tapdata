package com.tapdata.entity;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2020-07-29 11:41
 **/
public class LogReaderOffset implements Serializable {

	private static final long serialVersionUID = 2297261827698591792L;
	private long timestamp;

	public LogReaderOffset() {

	}

	public LogReaderOffset(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "LogReaderOffset{" +
				"timestamp=" + timestamp +
				'}';
	}
}
