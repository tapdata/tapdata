package com.tapdata.entity;

public class ResponseBody {
	private String code;

	private String msg;

	private Object data;

	private String reqId;

	private long ts;

	public ResponseBody() {
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public String getReqId() {
		return reqId;
	}

	public void setReqId(String reqId) {
		this.reqId = reqId;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("ResponseBody{");
		sb.append("code='").append(code).append('\'');
		sb.append(", msg='").append(msg).append('\'');
		sb.append(", data=").append(data);
		sb.append(", reqId='").append(reqId).append('\'');
		sb.append(", ts=").append(ts);
		sb.append('}');
		return sb.toString();
	}
}
