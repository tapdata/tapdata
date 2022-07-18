package com.tapdata.entity;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2020-12-23 16:59
 **/
public class Milestone implements Serializable {

	private static final long serialVersionUID = -2410729332532183963L;

	private String code;

	private String status;

	private String errorMessage;

	private Long start;

	private Long end;

	private String group;

	public Milestone() {
	}

	public Milestone(String code, String status, String group) {
		this.code = code;
		this.status = status;
		this.errorMessage = "";
		this.group = group;
	}

	public Milestone(String code, String status, Long start, String group) {
		this.code = code;
		this.status = status;
		this.start = start;
		this.group = group;
	}

	public Milestone(String code, String status, String errorMessage, Long start, Long end) {
		this.code = code;
		this.status = status;
		this.errorMessage = errorMessage;
		this.start = start;
		this.end = end;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public Long getStart() {
		return start;
	}

	public void setStart(Long start) {
		this.start = start;
	}

	public Long getEnd() {
		return end;
	}

	public void setEnd(Long end) {
		this.end = end;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;

		if (o == null || getClass() != o.getClass()) return false;

		Milestone milestone = (Milestone) o;

		return new EqualsBuilder().append(code, milestone.code).append(status, milestone.status).append(errorMessage, milestone.errorMessage).append(start, milestone.start).append(end, milestone.end).isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37).append(code).append(status).append(errorMessage).append(start).append(end).toHashCode();
	}

	@Override
	public String toString() {
		return "Milestone{" +
				"code='" + code + '\'' +
				", status='" + status + '\'' +
				", errorMessage='" + errorMessage + '\'' +
				", start=" + start +
				", end=" + end +
				'}';
	}
}
