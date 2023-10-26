package io.tapdata.exception;

import io.tapdata.entity.schema.value.DateTime;

/**
 * @author samuel
 * @Description
 * @create 2023-10-24 14:48
 **/
public class DatetimeFormatException extends RuntimeException {
	private final DateTime dateTime;
	private final String formatPattern;

	public DatetimeFormatException(DateTime dateTime, String formatPattern, Throwable cause) {
		super(cause);
		this.dateTime = dateTime;
		this.formatPattern = formatPattern;
	}

	@Override
	public String getMessage() {
		return String.format("Failed to convert date according to format, wait to convert datetime: %s, format pattern: %s",
				null == dateTime ? "Null datetime value" : dateTime,
				null == formatPattern || formatPattern.trim().isEmpty() ? "Blank format pattern" : formatPattern);
	}
}
