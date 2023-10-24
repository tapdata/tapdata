package io.tapdata.async.master;

/**
 * @author aplomb
 */
public interface AsyncJobCompleted {
	void completed(JobContext jobContext, Throwable throwable);
}
