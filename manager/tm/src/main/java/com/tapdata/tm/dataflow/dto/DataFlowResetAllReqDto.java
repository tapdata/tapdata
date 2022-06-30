/**
 * @title: DataFlowResetAllDto
 * @description:
 * @author lk
 * @date 2021/9/13
 */
package com.tapdata.tm.dataflow.dto;

import java.util.List;

public class DataFlowResetAllReqDto {

	private List<String> id;

	public List<String> getId() {
		return id;
	}

	public void setId(List<String> id) {
		this.id = id;
	}
}
