package io.tapdata.observable.logging.appender;

/**
 * @author samuel
 * @Description
 * @create 2023-03-30 17:11
 **/
public abstract class BaseTaskAppender<T> extends BaseAppender<T> {
	private static final long serialVersionUID = 2679449382830707234L;
	protected String taskId;

	public BaseTaskAppender(String taskId) {
		this.taskId = taskId;
	}

	public String getTaskId() {
		return taskId;
	}
}
