/**
 * @title: ValidatorSetting
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

public class ValidatorSetting {

	private String type;

	private Sampling condition;

	private ValidatorStage source;

	private ValidatorStage target;

	private String validateCode;

	public String getType() {
		return type;
	}

	public Sampling getCondition() {
		return condition;
	}

	public ValidatorStage getSource() {
		return source;
	}

	public ValidatorStage getTarget() {
		return target;
	}

	public String getValidateCode() {
		return validateCode;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setCondition(Sampling condition) {
		this.condition = condition;
	}

	public void setSource(ValidatorStage source) {
		this.source = source;
	}

	public void setTarget(ValidatorStage target) {
		this.target = target;
	}

	public void setValidateCode(String validateCode) {
		this.validateCode = validateCode;
	}
}
