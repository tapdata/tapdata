package io.tapdata.exception;

import io.tapdata.PDKExCode_10;

/**
 * @author samuel
 * @Description
 * @create 2023-04-27 16:08
 **/
public class TapDbCdcConfigInvalidEx extends TapPdkBaseException {

	protected TapDbCdcConfigInvalidEx(String pdkId, Throwable cause) {
		super(PDKExCode_10.DB_CDC_CONFIG_INVALID, pdkId, cause);
	}

	protected TapDbCdcConfigInvalidEx(String message, String pdkId, Throwable cause) {
		super(PDKExCode_10.DB_CDC_CONFIG_INVALID, message, pdkId, cause);
	}

	@Override
	public String getMessage() {
		// TODO
		return null;
	}

	@Override
	protected void clone(TapRuntimeException tapRuntimeException) {
		super.clone(tapRuntimeException);
		if (tapRuntimeException instanceof TapDbCdcConfigInvalidEx) {
			// TODO
		}
	}
}
