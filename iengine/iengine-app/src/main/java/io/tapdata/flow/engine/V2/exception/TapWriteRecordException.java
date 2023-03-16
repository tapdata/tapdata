package io.tapdata.flow.engine.V2.exception;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 11:18
 **/
public class TapWriteRecordException extends TapRecordException {

	public TapWriteRecordException(String code) {
		super(code);
	}

	public TapWriteRecordException(String code, String message) {
		super(code, message);
	}

	public TapWriteRecordException(String code, String message, Throwable cause) {
		super(code, message, cause);
	}

	public TapWriteRecordException(String code, Throwable cause) {
		super(code, cause);
	}
}
