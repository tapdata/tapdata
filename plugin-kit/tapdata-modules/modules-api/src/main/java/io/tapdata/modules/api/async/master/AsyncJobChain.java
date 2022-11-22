package io.tapdata.modules.api.async.master;

import java.util.Map;
import java.util.Set;

/**
 * @author aplomb
 */
public interface AsyncJobChain {
	AsyncJobChain add(Map.Entry<String, AsyncJob> asyncJobChain);
	AsyncJobChain add(String id, AsyncJob asyncJob);
	AsyncJob remove(String id);

	Set<Map.Entry<String, AsyncJob>> asyncJobs();
}
