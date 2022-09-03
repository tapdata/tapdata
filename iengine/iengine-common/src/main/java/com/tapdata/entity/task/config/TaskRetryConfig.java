package com.tapdata.entity.task.config;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * @author samuel
 * @Description
 * @create 2022-09-03 14:08
 **/
public class TaskRetryConfig implements Serializable {
	private static final long serialVersionUID = -6473260293451962641L;
	private Long retryIntervalSecond;

	public TaskRetryConfig retryIntervalSecond(Long retryIntervalSecond) {
		this.retryIntervalSecond = retryIntervalSecond;
		return this;
	}

	private Long maxRetryTimeSecond;

	public TaskRetryConfig maxRetryTimeSecond(Long maxRetryTimeSecond) {
		this.maxRetryTimeSecond = maxRetryTimeSecond;
		return this;
	}

	public static TaskRetryConfig create() {
		return new TaskRetryConfig();
	}

	public Long getRetryIntervalSecond() {
		return retryIntervalSecond;
	}

	public Long getMaxRetryTimeSecond() {
		return maxRetryTimeSecond;
	}

	public Long getMaxRetryTime(TimeUnit timeUnit) {
		if (null == maxRetryTimeSecond) {
			return 0L;
		}
		switch (timeUnit) {
			case SECONDS:
				return maxRetryTimeSecond;
			case MINUTES:
				return maxRetryTimeSecond / 60;
			default:
				throw new UnsupportedOperationException();
		}
	}
}
