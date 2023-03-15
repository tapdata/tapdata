package io.tapdata.flow.engine.V2.task.retry;

/**
 * @author samuel
 * @Description
 * @create 2023-03-11 17:13
 **/
public abstract class RetryService {
	protected RetryContext retryContext;

	public RetryService(RetryContext retryContext) {
		if (null == retryContext) {
			throw new IllegalArgumentException("Retry context cannot be null");
		}
		this.retryContext = retryContext;
	}

	abstract public void start();

	abstract public void reset();
}
