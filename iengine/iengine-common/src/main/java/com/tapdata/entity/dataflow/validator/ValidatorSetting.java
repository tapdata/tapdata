package com.tapdata.entity.dataflow.validator;


import java.io.Serializable;

/**
 * 用户设置的校验配置中的单条配置
 * Created by xj
 * 2020-04-16 01:25
 **/

public class ValidatorSetting implements Serializable {

	private static final long serialVersionUID = -8176866550234128124L;

	private String type; // row: 行数  hash：哈希  advance：高级校验

	private Sampling condition; //采样信息

	private ValidatorStage source; //源表

	private ValidatorStage target; //目标表

	private String validateCode; //高级校验涉及字段

	public String getType() {
		return type;
	}

	public String getValidateCode() {
		return validateCode;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setValidateCode(String validateCode) {
		this.validateCode = validateCode;
	}

	public ValidatorStage getSource() {
		return source;
	}

	public ValidatorStage getTarget() {
		return target;
	}

	public void setSource(ValidatorStage source) {
		this.source = source;
	}

	public void setTarget(ValidatorStage target) {
		this.target = target;
	}

	public Sampling getCondition() {
		return condition;
	}

	public void setCondition(Sampling condition) {
		this.condition = condition;
	}
}
