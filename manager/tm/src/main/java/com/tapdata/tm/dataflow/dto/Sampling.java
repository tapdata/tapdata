/**
 * @title: Sampling
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

public class Sampling {

	private String type;

	private Long value;

	public String getType() {
		return type;
	}

	public Long getValue() {
		return value;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setValue(Long value) {
		this.value = value;
	}
}
