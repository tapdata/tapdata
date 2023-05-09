package io.tapdata.flow.engine.V2.task.cleaner;

/**
 * @author samuel
 * @Description
 * @create 2022-10-13 16:41
 **/
public class TaskCleanerException extends Exception {

	private boolean canRetry;

	public TaskCleanerException(String message) {
		super(message);
	}

	public TaskCleanerException(Throwable cause) {
		super(cause);
	}

	public TaskCleanerException(String message, Throwable cause) {
		super(message, cause);
	}

	public TaskCleanerException(String message, boolean canRetry) {
		super(message);
		this.canRetry = canRetry;
	}

	public TaskCleanerException(Throwable cause, boolean canRetry) {
		super(cause);
		this.canRetry = canRetry;
	}

	public TaskCleanerException(String message, Throwable cause, boolean canRetry) {
		super(message, cause);
		this.canRetry = canRetry;
	}

	public boolean canRetry() {
		return canRetry;
	}
}
