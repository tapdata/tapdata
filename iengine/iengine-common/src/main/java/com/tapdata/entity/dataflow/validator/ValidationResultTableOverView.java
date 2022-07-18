/**
 * @title: ValidationResultTableOverView
 * @description:
 * @author lk
 * @date 2020/7/9
 */
package com.tapdata.entity.dataflow.validator;

import java.io.Serializable;
import java.util.List;

public class ValidationResultTableOverView implements Serializable {

	private static final long serialVersionUID = 3752908253076333594L;

	private String id;

	private String validateBatchId;

	private String type;

	private long validateTime;

	private String dataFlowId;

	private List<ValidateStats> validateStats;

	public String getId() {
		return id;
	}

	public String getType() {
		return type;
	}

	public long getValidateTime() {
		return validateTime;
	}

	public List<ValidateStats> getValidateStats() {
		return validateStats;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setValidateTime(long validateTime) {
		this.validateTime = validateTime;
	}

	public void setValidateStats(List<ValidateStats> validateStats) {
		this.validateStats = validateStats;
	}

	public String getDataFlowId() {
		return dataFlowId;
	}

	public void setDataFlowId(String dataFlowId) {
		this.dataFlowId = dataFlowId;
	}

	public String getValidateBatchId() {
		return validateBatchId;
	}

	public void setValidateBatchId(String validateBatchId) {
		this.validateBatchId = validateBatchId;
	}
}
