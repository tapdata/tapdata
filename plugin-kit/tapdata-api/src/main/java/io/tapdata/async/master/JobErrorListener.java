package io.tapdata.async.master;

/**
 * @author aplomb
 */
public interface JobErrorListener {
	void errorOccurred(String id, JobBase asyncJob, Throwable throwable);
}
