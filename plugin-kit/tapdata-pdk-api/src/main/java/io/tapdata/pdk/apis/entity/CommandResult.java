package io.tapdata.pdk.apis.entity;

import java.util.Map;

public class CommandResult {
	public static final String CODE_OK = "ok";
	public static final String CODE_ERROR = "error";
	/**
	 * Use "data" instead
	 */
	@Deprecated
	private Map<String, Object> result;
	private Object data;

	private String code;
	private String message;

	@Deprecated
	public CommandResult result(Map<String, Object> result) {
		this.result = result;
		return this;
	}

	public CommandResult result(Object data, String code, String message){
		this.code = code;
		this.message = message;
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

	public String getCode() {
		return code;
	}

	public void setCode(String code){
		this.code = code;
	}

	public void setMsg(String message) {
		this.message = message;
	}

	public String getMsg() {
		return this.message;
	}
}
