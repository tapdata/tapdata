package io.tapdata.async.master;

/**
 * @author aplomb
 */
public interface AsyncJobErrorListener {
	void errorOccurred(String id, AsyncJob asyncJob, Throwable throwable);
}
