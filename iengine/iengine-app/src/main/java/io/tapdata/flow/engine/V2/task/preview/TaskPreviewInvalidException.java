package io.tapdata.flow.engine.V2.task.preview;

/**
 * @author samuel
 * @Description
 * @create 2024-09-20 11:57
 **/
public class TaskPreviewInvalidException extends Exception {
	public TaskPreviewInvalidException() {
		super();
	}

	public TaskPreviewInvalidException(String message) {
		super(message);
	}

	public TaskPreviewInvalidException(String message, Throwable cause) {
		super(message, cause);
	}

	public TaskPreviewInvalidException(Throwable cause) {
		super(cause);
	}

	protected TaskPreviewInvalidException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
