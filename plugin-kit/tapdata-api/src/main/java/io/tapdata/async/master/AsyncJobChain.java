package io.tapdata.async.master;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * @author aplomb
 */
public interface AsyncJobChain {
	AsyncJobChain job(AsyncJob asyncJob);
	AsyncJobChain job(String id, AsyncJob asyncJob);

	AsyncJobChain job(String id, AsyncJob asyncJob, boolean pending);

	AsyncJobChain externalJob(String id, Function<JobContext, JobContext> jobContextConsumer);

	AsyncJobChain externalJob(String id, Function<JobContext, JobContext> jobContextConsumer, boolean pending);

	AsyncJobChain externalJob(String id, AsyncJob asyncJob, Function<JobContext, JobContext> jobContextConsumer, boolean pending);

	boolean isPending(String id);

	AsyncJob remove(String id);

	Set<Map.Entry<String, AsyncJob>> asyncJobs();
}
