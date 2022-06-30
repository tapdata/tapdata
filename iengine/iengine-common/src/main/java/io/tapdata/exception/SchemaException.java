package io.tapdata.exception;

public class SchemaException extends RuntimeException {

	private int level;

	public static final int ERROR = 1;
	public static final int WARN = 2;

	public SchemaException(int level) {
		this.level = level;
	}

	public SchemaException(String message, int level) {
		super(message);
		this.level = level;
	}

	public SchemaException(String message, Throwable cause, int level) {
		super(message, cause);
		this.level = level;
	}

	public SchemaException(Throwable cause, int level) {
		super(cause);
		this.level = level;
	}

	public SchemaException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, int level) {
		super(message, cause, enableSuppression, writableStackTrace);
		this.level = level;
	}

	public int getLevel() {
		return level;
	}
}
