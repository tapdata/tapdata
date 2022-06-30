/**
 * @title: RuntimeThroughput
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

public class RuntimeThroughput {

	private Long rows;

	private Long dataSize;

	public Long getRows() {
		return rows;
	}

	public Long getDataSize() {
		return dataSize;
	}

	public void setRows(Long rows) {
		this.rows = rows;
	}

	public void setDataSize(Long dataSize) {
		this.dataSize = dataSize;
	}
}
