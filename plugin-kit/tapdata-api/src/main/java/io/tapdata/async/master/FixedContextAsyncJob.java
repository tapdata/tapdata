package io.tapdata.async.master;

/**
 * @author aplomb
 */
public abstract class FixedContextAsyncJob<T> implements AsyncJob{
	protected T context;
	public JobContext run(JobContext jobContext) {
		context = (T) jobContext.getContext();
		return execute(jobContext);
	}

	protected abstract JobContext execute(JobContext jobContext);
}
