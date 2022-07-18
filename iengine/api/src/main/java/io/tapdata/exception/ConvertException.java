package io.tapdata.exception;

public class ConvertException extends Exception {

	private String errCode;

	public ConvertException(String errMessage) {
		super(errMessage);

	}

	public ConvertException(Throwable cause, String errMessage) {
		super(errMessage, cause);

	}

	public ConvertException(String errCode, String errMessage) {
		super(errCode + "-" + errMessage);
		this.errCode = errCode;
	}

	public ConvertException(Throwable cause, String errCode, String errMessage) {
		super(errCode + "-" + errMessage, cause);
		this.errCode = errCode;
	}

	public String getErrMessage() {
		return super.getMessage();
	}

	public String getErrCode() {
		return errCode;
	}
}
