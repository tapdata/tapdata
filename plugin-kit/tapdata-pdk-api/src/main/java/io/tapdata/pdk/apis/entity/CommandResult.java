package io.tapdata.pdk.apis.entity;

import java.util.Map;

public class CommandResult {
	public static final int CODE_OK = 1;
	public static final int CODE_ERROR = 0;
	/**
	 * Use "data" instead
	 */
	@Deprecated
	private Map<String, Object> result;
	private Object data;


	@Deprecated
	public CommandResult result(Map<String, Object> result) {
		this.result = result;
		return this;
	}

	public CommandResult result(Object data, int code, String message){
		this.data = data;
		return this;
	}



	@Override
	public String toString() {
		return "CommandResult result " + (data == null ? result : data) + ";";
	}

	@Deprecated
	public Map<String, Object> getResult() {
		return result;
	}

	@Deprecated
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
