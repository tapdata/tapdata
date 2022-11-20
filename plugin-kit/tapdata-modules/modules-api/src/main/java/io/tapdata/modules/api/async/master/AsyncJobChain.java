package io.tapdata.modules.api.async.master;

/**
 * @author aplomb
 */
public interface AsyncJobChain {
	AsyncJobChain add(String id, AsyncJob asyncJob);
}
