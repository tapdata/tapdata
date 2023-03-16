package io.tapdata.pdk.apis.error;

import io.tapdata.exception.TapCodeException;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 14:46
 **/
public abstract class TapPDKException extends TapCodeException {
	public TapPDKException(String code) {
		super(code);
	}

	public TapPDKException(String code, String message) {
		super(code, message);
	}
}
