package io.tapdata.exception;

import io.tapdata.PDKExCode_10;
import io.tapdata.exception.TapCodeException;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 15:15
 **/
public class TapPDKOffsetOutOfLogException extends TapCodeException {
	private final Object offset;

	public TapPDKOffsetOutOfLogException(Object offset, Throwable cause) {
		super(PDKExCode_10.OFFSET_OUT_OF_LOG, cause);
		this.offset = offset;
	}

	@Override
	public String getMessage() {
		if (null != offset) {
			return String.format("Start point out of log: %s", offset);
		} else {
			return "Start point out of log, offset is null";
		}
	}
}
