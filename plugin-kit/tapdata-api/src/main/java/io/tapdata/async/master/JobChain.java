package io.tapdata.async.master;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * @author aplomb
 */
public interface JobChain {
	JobChain job(Job asyncJob);
	JobChain job(String id, Job asyncJob);

	JobChain job(String id, Job asyncJob, boolean pending);

	JobChain externalJob(String id, Function<JobContext, JobContext> jobContextConsumer);

	JobChain externalJob(String id, Function<JobContext, JobContext> jobContextConsumer, boolean pending);

	JobChain externalJob(String id, Job asyncJob, Function<JobContext, JobContext> jobContextConsumer, boolean pending);
	JobChain asyncJob(String id, AsyncJob asyncJob);
	JobChain asyncJob(String id, AsyncJob asyncJob, boolean pending);
	boolean isPending(String id);

	JobBase remove(String id);

	Set<Map.Entry<String, JobBase>> asyncJobs();
}
