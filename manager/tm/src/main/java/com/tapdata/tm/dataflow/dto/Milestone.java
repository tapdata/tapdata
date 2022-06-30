/**
 * @title: Milestone
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

public class Milestone {

	private String code;

	private String status;

	private String errorMessage;

	private Long start;

	private Long end;

	public String getCode() {
		return code;
	}

	public String getStatus() {
		return status;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public Long getStart() {
		return start;
	}

	public Long getEnd() {
		return end;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public void setStart(Long start) {
		this.start = start;
	}

	public void setEnd(Long end) {
		this.end = end;
	}
}
