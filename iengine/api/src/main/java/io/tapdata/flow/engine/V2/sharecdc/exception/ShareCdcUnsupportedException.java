package io.tapdata.flow.engine.V2.sharecdc.exception;

/**
 * @author samuel
 * @Description
 * @create 2022-02-17 15:51
 **/
public class ShareCdcUnsupportedException extends Exception {

	private boolean continueWithNormalCdc;

	public ShareCdcUnsupportedException(boolean continueWithNormalCdc) {
		this.continueWithNormalCdc = continueWithNormalCdc;
	}

	public ShareCdcUnsupportedException(String message, boolean continueWithNormalCdc) {
		super(message);
		this.continueWithNormalCdc = continueWithNormalCdc;
	}

	public ShareCdcUnsupportedException(String message, Throwable cause, boolean continueWithNormalCdc) {
		super(message, cause);
		this.continueWithNormalCdc = continueWithNormalCdc;
	}

	public ShareCdcUnsupportedException(Throwable cause, boolean continueWithNormalCdc) {
		super(cause);
		this.continueWithNormalCdc = continueWithNormalCdc;
	}

	public ShareCdcUnsupportedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, boolean continueWithNormalCdc) {
		super(message, cause, enableSuppression, writableStackTrace);
		this.continueWithNormalCdc = continueWithNormalCdc;
	}

	public boolean isContinueWithNormalCdc() {
		return continueWithNormalCdc;
	}
}
