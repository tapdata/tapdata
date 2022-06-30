package com.tapdata.entity.dataflow;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author jackin
 */
public class JsDebugRowResult implements Serializable {

	private static final long serialVersionUID = -1623707751361044758L;

	private List<Map> params;
	/**
	 * js 返回结果
	 */
	private Object result;

	/**
	 * 执行结果
	 */
	private String status;

	/**
	 * 执行耗时
	 */
	private long time;

	/**
	 * 数据来源的stageId
	 */
	private String srcStageId;

	private List<JsDebugLog> out;

	public JsDebugRowResult(Object result, String status, long time, List<JsDebugLog> out) {
		this.result = result;
		this.status = status;
		this.time = time;
		this.out = out;
	}

	public JsDebugRowResult() {
	}

	public Object getResult() {
		return result;
	}

	public void setResult(Object result) {
		this.result = result;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public List<JsDebugLog> getOut() {
		return out;
	}

	public void setOut(List<JsDebugLog> out) {
		this.out = out;
	}

	public String getSrcStageId() {
		return srcStageId;
	}

	public void setSrcStageId(String srcStageId) {
		this.srcStageId = srcStageId;
	}

	public List<Map> getParams() {
		return params;
	}

	public void setParams(List<Map> params) {
		this.params = params;
	}
}
