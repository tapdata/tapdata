package com.tapdata.entity;

/**
 * @author samuel
 * @Description
 * @create 2021-12-15 12:04
 **/
public enum WorkerTypeEnum {
	CONNECTOR("connector"),
	TRANSFORMER("transformer"),
	;

	private String type;

	WorkerTypeEnum(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}
}
