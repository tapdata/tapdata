package com.tapdata.entity.dataflow.validator;


import java.io.Serializable;

/**
 * 采样模式
 * Created by xj
 * 2020-04-16 01:59
 **/

public class Sampling implements Serializable {

	private static final long serialVersionUID = -5202967405210081976L;

	private String type;

	private long value;

	public String getType() {
		return type;
	}

	public long getValue() {
		return value;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setValue(long value) {
		this.value = value;
	}
}
