package io.tapdata.flow.engine.V2.task.preview;

/**
 * @author samuel
 * @Description
 * @create 2024-09-19 12:19
 **/
public class TaskPreviewException extends Exception {
	public TaskPreviewException() {
		super();
	}

	public TaskPreviewException(String message) {
		super(message);
	}

	public TaskPreviewException(String message, Throwable cause) {
		super(message, cause);
	}

	public TaskPreviewException(Throwable cause) {
		super(cause);
	}

	protected TaskPreviewException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
