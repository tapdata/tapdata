/**
 * @title: ValidationResultFailed
 * @description:
 * @author lk
 * @date 2020/7/8
 */
package com.tapdata.entity.dataflow.validator;

import java.io.Serializable;

public class ValidationResultFailedRow implements Serializable {

	private static final long serialVersionUID = -3500790147354835190L;

	private String id;

	private String validateBatchId;

	private String type;

	private long validateTime;

	private String dataFlowId;

	private String validateType;

	private ValidatorStage sourceStage;

	private String sourceTableData;

	private ValidatorStage targetStage;

	private String targetTableData;

	private String message;

	public String getId() {
		return id;
	}

	public String getType() {
		return type;
	}

	public long getValidateTime() {
		return validateTime;
	}

	public String getDataFlowId() {
		return dataFlowId;
	}

	public String getValidateType() {
		return validateType;
	}

	public ValidatorStage getSourceStage() {
		return sourceStage;
	}

	public String getSourceTableData() {
		return sourceTableData;
	}

	public ValidatorStage getTargetStage() {
		return targetStage;
	}

	public String getTargetTableData() {
		return targetTableData;
	}

	public String getMessage() {
		return message;
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

	public void setDataFlowId(String dataFlowId) {
		this.dataFlowId = dataFlowId;
	}

	public void setValidateType(String validateType) {
		this.validateType = validateType;
	}

	public void setSourceStage(ValidatorStage sourceStage) {
		this.sourceStage = sourceStage;
	}

	public void setSourceTableData(String sourceTableData) {
		this.sourceTableData = sourceTableData;
	}

	public void setTargetStage(ValidatorStage targetStage) {
		this.targetStage = targetStage;
	}

	public void setTargetTableData(String targetTableData) {
		this.targetTableData = targetTableData;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getValidateBatchId() {
		return validateBatchId;
	}

	public void setValidateBatchId(String validateBatchId) {
		this.validateBatchId = validateBatchId;
	}
}
