package io.tapdata.error;

import io.tapdata.exception.TapCodeException;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
public class TapUnknownException extends TapCodeException {
	public TapUnknownException(Throwable cause) {
		super(TapIEngineExCode_11.UNKNOWN_ERROR, cause);
	}
}
