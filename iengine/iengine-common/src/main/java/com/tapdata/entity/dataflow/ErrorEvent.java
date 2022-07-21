package com.tapdata.entity.dataflow;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author samuel
 * @Description
 * @create 2020-11-10 23:26
 **/
public class ErrorEvent implements Serializable {

	private static final long serialVersionUID = 7222472454805919082L;

	private String message;

	private String[] stacks;

	private String jobId;

	private String type;

	private String loggerName;

	private boolean hit;

	public ErrorEvent() {
	}

	public ErrorEvent(String message, String[] stacks, String jobId, String type, String loggerName) {
		this.message = message;
		this.stacks = stacks;
		this.jobId = jobId;
		this.type = type;
		this.loggerName = loggerName;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String[] getStacks() {
		return stacks;
	}

	public void setStacks(String[] stacks) {
		this.stacks = stacks;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getLoggerName() {
		return loggerName;
	}

	public void setLoggerName(String loggerName) {
		this.loggerName = loggerName;
	}

	public boolean getHit() {
		return hit;
	}

	public void setHit(boolean hit) {
		this.hit = hit;
	}

	@Override
	public String toString() {
		return "ErrorEvent{" +
				"message='" + message + '\'' +
				", stacks=" + Arrays.toString(stacks) +
				", jobId='" + jobId + '\'' +
				", type='" + type + '\'' +
				", loggerName='" + loggerName + '\'' +
				'}';
	}
}
