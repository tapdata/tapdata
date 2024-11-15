package io.tapdata.flow.engine.V2.task.preview;

/**
 * @author samuel
 * @Description
 * @create 2024-09-23 14:54
 **/
public class TaskPreviewParseException extends Exception {
	public TaskPreviewParseException() {
		super();
	}

	public TaskPreviewParseException(String message) {
		super(message);
	}

	public TaskPreviewParseException(String message, Throwable cause) {
		super(message, cause);
	}

	public TaskPreviewParseException(Throwable cause) {
		super(cause);
	}

	protected TaskPreviewParseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
