package io.tapdata.exception;

import com.tapdata.entity.ResponseBody;

public class RestException extends RuntimeException {

	private String uri;
	private String method;
	private Object param;
	private String code;
	private Object data;
	private String reqId;

	public String getUri() {
		return uri;
	}

	public String getMethod() {
		return method;
	}

	public Object getParam() {
		return param;
	}

	public String getCode() {
		return code;
	}

	public Object getData() {
		return data;
	}

	public RestException(String uri, String method, Object param, ResponseBody responseBody) {
		super(responseBody.getMessage());
		this.uri = uri;
		this.method = method;
		this.param = param;
		this.code = responseBody.getCode();
		this.data = responseBody.getData();
		this.reqId = responseBody.getReqId();
	}

	public RestException(Throwable cause, String uri, String method, Object param, ResponseBody responseBody) {
		super(responseBody.getMessage(), cause);
		this.uri = uri;
		this.method = method;
		this.param = param;
		this.code = responseBody.getCode();
		this.data = responseBody.getData();
		this.reqId = responseBody.getReqId();
	}

	@Override
	public String getMessage() {
		return toString();
	}

	public boolean isCode(String code) {
		return code.equals(getCode());
	}

	@Override
	public String toString() {
		return "RestException{" +
				"uri='" + uri + '\'' +
				", method='" + method + '\'' +
				", param=" + param +
				", code='" + code + '\'' +
				", data=" + data +
				", reqId=" + reqId +
				"}: " + super.getMessage();
	}
}
