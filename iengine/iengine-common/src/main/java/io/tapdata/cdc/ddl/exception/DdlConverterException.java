package io.tapdata.cdc.ddl.exception;

import io.tapdata.cdc.ddl.DdlEvent;

/**
 * DDL转换异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/12 上午4:07 Create
 */
public class DdlConverterException extends DdlException {
	private DdlEvent event;

	public DdlConverterException(DdlEvent event) {
		this.event = event;
	}

	public DdlConverterException(String message, DdlEvent event) {
		super(message);
		this.event = event;
	}

	public DdlConverterException(String message, Throwable cause, DdlEvent event) {
		super(message, cause);
		this.event = event;
	}

	public DdlConverterException(Throwable cause, DdlEvent event) {
		super(cause);
		this.event = event;
	}

	public DdlEvent getEvent() {
		return event;
	}
}
