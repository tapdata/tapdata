/**
 * @title: LogCollectorSetting
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

import java.util.List;

public class LogCollectorSetting {

	private String connectionId;

	private List<String> includeTables;

	private String selectType;

	public String getConnectionId() {
		return connectionId;
	}

	public List<String> getIncludeTables() {
		return includeTables;
	}

	public String getSelectType() {
		return selectType;
	}

	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}

	public void setIncludeTables(List<String> includeTables) {
		this.includeTables = includeTables;
	}

	public void setSelectType(String selectType) {
		this.selectType = selectType;
	}
}
