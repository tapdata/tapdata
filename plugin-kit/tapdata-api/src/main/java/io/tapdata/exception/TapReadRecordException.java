package io.tapdata.exception;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 11:18
 **/
public class TapReadRecordException extends TapRecordException{

	public TapReadRecordException(String code) {
		super(code);
	}

	public TapReadRecordException(String code, String message) {
		super(code, message);
	}

	public TapReadRecordException(String code, String message, Throwable cause) {
		super(code, message, cause);
	}

	public TapReadRecordException(String code, Throwable cause) {
		super(code, cause);
	}
}
