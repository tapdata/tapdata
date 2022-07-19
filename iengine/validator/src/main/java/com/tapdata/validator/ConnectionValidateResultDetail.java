package com.tapdata.validator;

public class ConnectionValidateResultDetail {

	private String stage_code;

	private String show_msg;

	private String status = ValidatorConstant.VALIDATE_DETAIL_RESULT_WAITING;

	private int sort;

	private String error_code;

	private String fail_message;

	private boolean required;

	/**
	 * the connection validation time cost, in mills
	 */
	private long cost;

	public ConnectionValidateResultDetail() {
	}

	public ConnectionValidateResultDetail(String stage_code, String show_msg, int sort, boolean required) {
		this.stage_code = stage_code;
		this.show_msg = show_msg;
		this.sort = sort;
		this.required = required;
	}

	public void failed(String fail_message) {
		this.status = ValidatorConstant.VALIDATE_DETAIL_RESULT_FAIL;
		this.fail_message = fail_message;
	}

	public void failed(String fail_message, String error_code) {
		this.error_code = error_code;
		this.status = ValidatorConstant.VALIDATE_DETAIL_RESULT_FAIL;
		this.fail_message = fail_message;
	}

	public String getStage_code() {
		return stage_code;
	}

	public void setStage_code(String stage_code) {
		this.stage_code = stage_code;
	}

	public String getShow_msg() {
		return show_msg;
	}

	public void setShow_msg(String show_msg) {
		this.show_msg = show_msg;
	}

	public String getError_code() {
		return error_code;
	}

	public void setError_code(String error_code) {
		this.error_code = error_code;
	}

	public String getFail_message() {
		return fail_message;
	}

	public void setFail_message(String fail_message) {
		this.fail_message = fail_message;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public int getSort() {
		return sort;
	}

	public void setSort(int sort) {
		this.sort = sort;
	}

	public boolean getRequired() {
		return required;
	}

	public void setRequired(boolean required) {
		this.required = required;
	}


	public long getCost() {
		return cost;
	}

	public void setCost(long cost) {
		this.cost = cost;
	}
}
