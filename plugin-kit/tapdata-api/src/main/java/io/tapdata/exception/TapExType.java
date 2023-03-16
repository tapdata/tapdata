package io.tapdata.exception;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 11:57
 **/
public enum TapExType {
	RUNTIME("Runtime exception"),
	IO("An io exception"),
	NPE("Java null pointer exception"),
	DATE_FORMAT("Date format exception"),
	CANNOT_NULL("Cannot null exception"),
	CANNOT_EMPTY("Cannot empty exception"),
	CANNOT_BLANK("String cannot blank exception"),
	PARAMETER_INVALID("Some parameter is invalid"),
	;

	private String describe;

	TapExType(String describe) {
		this.describe = describe;
	}

	public String getDescribe() {
		return describe;
	}
}
