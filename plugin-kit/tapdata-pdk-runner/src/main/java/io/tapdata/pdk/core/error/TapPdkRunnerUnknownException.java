package io.tapdata.pdk.core.error;

import io.tapdata.exception.TapCodeException;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:32
 **/
public class TapPdkRunnerUnknownException extends TapCodeException {
	private static final long serialVersionUID = 4820551931387403402L;
	public TapPdkRunnerUnknownException(Throwable cause) {
		super(TapPdkRunnerExCode_18.UNKNOWN_ERROR, cause);
	}
}
