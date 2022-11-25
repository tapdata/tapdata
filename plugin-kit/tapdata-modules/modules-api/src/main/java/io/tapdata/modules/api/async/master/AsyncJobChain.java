package io.tapdata.modules.api.async.master;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * @author aplomb
 */
public interface AsyncJobChain {
	AsyncJobChain job(Map.Entry<String, AsyncJob> asyncJobChain);
	AsyncJobChain job(String id, AsyncJob asyncJob);

	AsyncJobChain externalJob(String id, Function<JobContext, JobContext> jobContextConsumer);

	AsyncJob remove(String id);

	Set<Map.Entry<String, AsyncJob>> asyncJobs();
}
