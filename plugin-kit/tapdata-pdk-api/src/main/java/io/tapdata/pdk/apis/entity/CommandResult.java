package io.tapdata.pdk.apis.entity;

import java.util.Date;
import java.util.Map;

public class CommandResult {
	private Map<String, Object> result;
	public CommandResult result(Map<String, Object> result) {
		this.result = result;
		return this;
	}

	@Override
	public String toString() {
		return "CommandResult result " + result + ";";
	}

	public Map<String, Object> getResult() {
		return result;
	}

	public void setResult(Map<String, Object> result) {
		this.result = result;
	}
}
