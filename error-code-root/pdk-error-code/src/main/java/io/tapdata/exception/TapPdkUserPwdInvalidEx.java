package io.tapdata.exception;

import io.tapdata.PDKExCode_10;

/**
 * @author samuel
 * @Description
 * @create 2023-03-17 21:34
 **/
public class TapPdkUserPwdInvalidEx extends TapPdkBaseException {

	private static final long serialVersionUID = 4424839146847138114L;

	private String username;

	public TapPdkUserPwdInvalidEx(String pdkId, String username, Throwable cause) {
		super(PDKExCode_10.USERNAME_PASSWORD_INVALID, pdkId, cause);
		this.username = username;
	}

	public String getUsername() {
		return username;
	}

	@Override
	public String getMessage() {
		return String.format("Unable to connect to %s, incorrect username or password, username: %s", pdkId, username);
	}

	@Override
	protected void clone(TapRuntimeException tapRuntimeException) {
		if (tapRuntimeException instanceof TapPdkUserPwdInvalidEx) {
			TapPdkUserPwdInvalidEx tapPDKUserPwdInvalidEx = (TapPdkUserPwdInvalidEx) tapRuntimeException;
			tapPDKUserPwdInvalidEx.username = this.username;
		}
		super.clone(tapRuntimeException);
	}
}
