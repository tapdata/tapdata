/**
 * @title: ValidationResultOverView
 * @description:
 * @author lk
 * @date 2020/7/9
 */
package com.tapdata.entity.dataflow.validator;

import java.io.Serializable;

public class ValidationResultOverView implements Serializable {

	private static final long serialVersionUID = 3554913911938092480L;

	private String id;

	private String validateBatchId;

	private String type;

	private long validateTime;

	private long costTime;

	private long validateRows;

	private long validateHashRows;

	private long validateJsRows;

	private long rowsDiffer;

	private long rowsMismatch;

	private double consistencyRate;

	private String dataFlowId;

	public String getId() {
		return id;
	}

	public String getType() {
		return type;
	}

	public long getValidateTime() {
		return validateTime;
	}

	public long getCostTime() {
		return costTime;
	}

	public long getValidateRows() {
		return validateRows;
	}

	public long getValidateHashRows() {
		return validateHashRows;
	}

	public long getValidateJsRows() {
		return validateJsRows;
	}

	public long getRowsDiffer() {
		return rowsDiffer;
	}

	public long getRowsMismatch() {
		return rowsMismatch;
	}

	public double getConsistencyRate() {
		return consistencyRate;
	}

	public String getDataFlowId() {
		return dataFlowId;
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

	public void setCostTime(long costTime) {
		this.costTime = costTime;
	}

	public void setValidateRows(long validateRows) {
		this.validateRows = validateRows;
	}

	public void setValidateHashRows(long validateHashRows) {
		this.validateHashRows = validateHashRows;
	}

	public void setValidateJsRows(long validateJsRows) {
		this.validateJsRows = validateJsRows;
	}

	public void setRowsDiffer(long rowsDiffer) {
		this.rowsDiffer = rowsDiffer;
	}

	public void setRowsMismatch(long rowsMismatch) {
		this.rowsMismatch = rowsMismatch;
	}

	public void setConsistencyRate(double consistencyRate) {
		this.consistencyRate = consistencyRate;
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
