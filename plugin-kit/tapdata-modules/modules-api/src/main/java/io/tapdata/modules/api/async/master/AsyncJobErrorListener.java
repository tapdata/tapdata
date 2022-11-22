package io.tapdata.modules.api.async.master;

/**
 * @author aplomb
 */
public interface AsyncJobErrorListener {
	void errorOccurred(String id, AsyncJob asyncJob, Throwable throwable);
}
