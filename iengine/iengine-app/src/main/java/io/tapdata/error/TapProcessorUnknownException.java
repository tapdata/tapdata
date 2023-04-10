package io.tapdata.error;

import io.tapdata.exception.TapCodeException;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
public class TapProcessorUnknownException extends TapCodeException {

	private static final long serialVersionUID = 6264341460648929466L;

	public TapProcessorUnknownException(Throwable cause) {
		super(TaskProcessorExCode_11.UNKNOWN_ERROR, cause);
	}
}
