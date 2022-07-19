package io.tapdata.entity;

public class BaseConnectionValidateResultDetail {

	public static final String VALIDATE_DETAIL_RESULT_PASSED = "passed";
	public static final String VALIDATE_DETAIL_RESULT_FAIL = "failed";
	public static final String VALIDATE_DETAIL_RESULT_WAITING = "waiting";

	private String show_msg;

	private String status = VALIDATE_DETAIL_RESULT_WAITING;

	private String fail_message;

	private boolean required;

	private String code;

	/**
	 * the connection validation time cost, in mills
	 */
	private long cost;

	public BaseConnectionValidateResultDetail() {
	}

	public BaseConnectionValidateResultDetail(String show_msg, String fail_message, boolean required) {
		this.show_msg = show_msg;
		this.fail_message = fail_message;
		this.required = required;
	}

	public BaseConnectionValidateResultDetail(String show_msg, String fail_message, boolean required, String code) {
		this.show_msg = show_msg;
		this.fail_message = fail_message;
		this.required = required;
		this.code = code;
	}

	public BaseConnectionValidateResultDetail(String show_msg, boolean required, String code) {
		this.show_msg = show_msg;
		this.required = required;
		this.code = code;
	}

	public void setPassedInfo() {
		this.status = VALIDATE_DETAIL_RESULT_PASSED;
	}

	public void setFailedInfo(String fail_message) {
		this.status = VALIDATE_DETAIL_RESULT_FAIL;
		this.fail_message = fail_message;
	}

	public String getShow_msg() {
		return show_msg;
	}

	public void setShow_msg(String show_msg) {
		this.show_msg = show_msg;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getFail_message() {
		return fail_message;
	}

	public void setFail_message(String fail_message) {
		this.fail_message = fail_message;
	}

	public boolean isRequired() {
		return required;
	}

	public void setRequired(boolean required) {
		this.required = required;
	}

	public String getCode() {
		return code;
	}

	public long getCost() {
		return cost;
	}

	public void setCost(long cost) {
		this.cost = cost;
	}

	@Override
	public String toString() {
		return "BaseConnectionValidateResultDetail{" +
				"show_msg='" + show_msg + '\'' +
				", status='" + status + '\'' +
				", fail_message='" + fail_message + '\'' +
				", required=" + required +
				", code='" + code + '\'' +
				'}';
	}
}
