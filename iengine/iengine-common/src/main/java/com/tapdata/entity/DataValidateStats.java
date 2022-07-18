package com.tapdata.entity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class DataValidateStats implements Serializable {
	private String jodId;
	private String jobName;
	private Map<String, Object> sourceInfo;
	private Map<String, Object> targetInfo;
	private List<DataValidateResult> validateResult;
	private String validateDate;

	public String getJodId() {
		return jodId;
	}

	public void setJodId(String jodId) {
		this.jodId = jodId;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public Map<String, Object> getSourceInfo() {
		return sourceInfo;
	}

	public void setSourceInfo(Map<String, Object> sourceInfo) {
		this.sourceInfo = sourceInfo;
	}

	public Map<String, Object> getTargetInfo() {
		return targetInfo;
	}

	public void setTargetInfo(Map<String, Object> targetInfo) {
		this.targetInfo = targetInfo;
	}

	public List<DataValidateResult> getValidateResult() {
		return validateResult;
	}

	public void setValidateResult(List<DataValidateResult> validateResult) {
		this.validateResult = validateResult;
	}

	public String getValidateDate() {
		return validateDate;
	}

	public void setValidateDate(String validateDate) {
		this.validateDate = validateDate;
	}
}
