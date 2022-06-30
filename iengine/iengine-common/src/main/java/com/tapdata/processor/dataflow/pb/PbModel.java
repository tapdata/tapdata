package com.tapdata.processor.dataflow.pb;

import lombok.Data;

import java.util.List;

@Data
public class PbModel {

	private String name;

	private List<PbProperty> propertyList;

	/**
	 * 复杂类型时，类型的属性
	 */
	private List<PbModel> nestedList;

}
