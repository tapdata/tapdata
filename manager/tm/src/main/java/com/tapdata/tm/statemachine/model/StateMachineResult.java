/**
 * @title: StateMachineResult
 * @description:
 * @author lk
 * @date 2021/11/30
 */
package com.tapdata.tm.statemachine.model;

import java.util.Objects;

public class StateMachineResult {

	private static final String OK = "ok";
	private static final String FAIL = "fail";

	private String code;

	private long modifiedCount;

	private String before;

	private String after;

	private String message;

	private StateMachineResult(String code, long modifiedCount, String message) {
		this.code = code;
		this.modifiedCount = modifiedCount;
		this.message = message;
	}

	public String getCode() {
		return code;
	}

	public long getModifiedCount() {
		return modifiedCount;
	}

	public String getMessage() {
		return message;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public void setModifiedCount(long modifiedCount) {
		this.modifiedCount = modifiedCount;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public static StateMachineResult ok(){
		return ok(0);
	}

	public static StateMachineResult ok(long modifiedCount){
		return new StateMachineResult(OK, modifiedCount, null);
	}

	public static StateMachineResult fail(String message){
		return new StateMachineResult(FAIL, 0, message);
	}


	public boolean isOk() {
		return Objects.equals(this.code, OK);
	}

	public boolean isFail() {
		return Objects.equals(this.code, FAIL);
	}

	public String getBefore() {
		return before;
	}

	public void setBefore(String before) {
		this.before = before;
	}

	public String getAfter() {
		return after;
	}

	public void setAfter(String after) {
		this.after = after;
	}
}
