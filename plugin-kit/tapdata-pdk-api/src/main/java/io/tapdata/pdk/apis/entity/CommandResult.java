package io.tapdata.pdk.apis.entity;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CommandResult {
	/**
	 * Use "data" instead
	 */
	@Deprecated
	private Map<String, Object> result;
	private Object data;
	public CommandResult result(Map<String, Object> result) {
		this.result = result;
		return this;
	}

	@Override
	public String toString() {
		return "CommandResult result " + (data == null ? result : data) + ";";
	}

	public Map<String, Object> getResult() {
		return result;
	}

	public void setResult(Map<String, Object> result) {
		this.result = result;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}
}
