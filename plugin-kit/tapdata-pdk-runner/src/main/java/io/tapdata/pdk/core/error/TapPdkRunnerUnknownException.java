package io.tapdata.pdk.core.error;

import io.tapdata.exception.TapCodeException;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:32
 **/
public class TapPdkRunnerUnknownException extends TapCodeException {
	public TapPdkRunnerUnknownException(Throwable cause) {
		super(TapPdkRunnerExCode_13.UNKNOWN_ERROR, cause);
	}
}
