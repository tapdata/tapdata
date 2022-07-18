package com.tapdata.entity;

import java.io.Serializable;

/**
 * 任务相关的消息的payload
 *
 * @author jackin
 * @date 2021/1/19 4:12 PM
 **/
public class JobMessagePayload implements Serializable {

	private static final long serialVersionUID = -6500509862786000941L;

	/**
	 * op 为 job_error的消息使用，表示任务出错的异常
	 */
	private Throwable jobErrorCause;

	/**
	 * 告警事件
	 */
	private Event emailEvent;

	public Throwable getJobErrorCause() {
		return jobErrorCause;
	}

	public void setJobErrorCause(Throwable jobErrorCause) {
		this.jobErrorCause = jobErrorCause;
	}

	public Event getEmailEvent() {
		return emailEvent;
	}

	public void setEmailEvent(Event emailEvent) {
		this.emailEvent = emailEvent;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("JobMessagePayload{");
		sb.append("jobErrorCause=").append(jobErrorCause);
		sb.append(", emailEvent=").append(emailEvent);
		sb.append('}');
		return sb.toString();
	}
}
