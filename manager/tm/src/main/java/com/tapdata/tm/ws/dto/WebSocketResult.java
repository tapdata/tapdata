/**
 * @title: WebSocketResult
 * @description:
 * @author lk
 * @date 2022/2/16
 */
package com.tapdata.tm.ws.dto;

public class WebSocketResult {

	private static final String OK = "ok";
	private static final String FAIL = "fail";

	private String code;

	private Object data;

	private String message;

	private String type;

	public WebSocketResult(String code, Object data, String message, String type) {
		this.code = code;
		this.data = data;
		this.message = message;
		this.type = type;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public static WebSocketResult ok(){
		return ok(null);
	}

	public static WebSocketResult ok(Object data){
		return ok(data, null);
	}

	public static WebSocketResult fail(String message){
		return fail(message, null);
	}

	public static WebSocketResult ok(Object data, String type){
		return new WebSocketResult(OK, data, null, type);
	}

	public static WebSocketResult fail(String message, String type){
		return new WebSocketResult(FAIL, null, message, type);
	}

	@Override
	public String toString() {
		return "WebSocketResult{" +
				"code='" + code + '\'' +
				", data=" + data +
				", message='" + message + '\'' +
				", type='" + type + '\'' +
				'}';
	}
}
