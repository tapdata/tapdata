package com.tapdata.processor.dataflow.pb;

import lombok.Data;

@Data
public class PbProperty {

	/**
	 * 字段规则
	 */
	private String label;

	/**
	 * 字段类型
	 */
	private String type;

	/**
	 * 字段名称
	 */
	private String name;

	/**
	 * 字段序号
	 */
	private int number;
}
