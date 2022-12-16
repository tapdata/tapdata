package io.tapdata.async.master;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.modules.api.async.master.*;

import java.util.*;
import java.util.function.Function;

/**
 * @author aplomb
 */
public class AsyncJobChainImpl implements AsyncJobChain {
	final Map<String, AsyncJob> asyncJobLinkedMap = Collections.synchronizedMap(new LinkedHashMap<>());
	final Set<String> pendingJobIds = Collections.synchronizedSet(new HashSet<>());
	private final Map<String, Class<? extends AsyncJob>> asyncJobMap;
	private final TapUtils tapUtils;
	public AsyncJobChainImpl(Map<String, Class<? extends AsyncJob>> asyncJobMap) {
		this.asyncJobMap = asyncJobMap;
		tapUtils = InstanceFactory.instance(TapUtils.class);
	}

	public Map<String, AsyncJob> cloneChain() {
		return Collections.synchronizedMap(new LinkedHashMap<>(asyncJobLinkedMap));
	}

	@Override
	public AsyncJobChain job(AsyncJob asyncJob) {
		return job(UUID.randomUUID().toString(), asyncJob);
	}

	@Override
	public AsyncJobChain job(String id, AsyncJob asyncJob) {
		return job(id, asyncJob, false);
	}
	@Override
	public AsyncJobChain job(String id, AsyncJob asyncJob, boolean pending) {
		asyncJobLinkedMap.put(id, asyncJob);
		if(pending) {
			pendingJobIds.add(id);
		}
		return this;
	}

	@Override
	public AsyncJobChain externalJob(String id, Function<JobContext, JobContext> jobContextConsumer) {
		return externalJob(id, jobContextConsumer, false);
	}
	@Override
	public AsyncJobChain externalJob(String id, Function<JobContext, JobContext> jobContextConsumer, boolean pending) {
		return externalJob(id, null, jobContextConsumer, pending);
	}
	@Override
	public AsyncJobChain externalJob(String id, AsyncJob asyncJob, Function<JobContext, JobContext> jobContextConsumer, boolean pending) {
		if(asyncJob == null) {
			Class<? extends AsyncJob> jobClass = asyncJobMap.get(id);
			if(jobClass == null)
				throw new CoreException(AsyncErrors.MISSING_JOB_CLASS_FOR_TYPE, "Job class is missing for type {}", id);
			try {
				asyncJob = jobClass.getConstructor().newInstance();
			} catch (Throwable e) {
				throw new CoreException(AsyncErrors.INITIATE_JOB_CLASS_FAILED, "Initiate job class {} failed, {}", jobClass, tapUtils.getStackTrace(e));
			}
		}
		AsyncJob finalAsyncJob = asyncJob;
		job(id, jobContext -> {
			JobContext context = finalAsyncJob.run(jobContext);
			return jobContextConsumer.apply(context);
		}, pending);
		return this;
	}

	@Override
	public boolean isPending(String id) {
		return pendingJobIds.contains(id);
	}

	@Override
	public AsyncJob remove(String id) {
		return asyncJobLinkedMap.remove(id);
	}

	@Override
	public Set<Map.Entry<String, AsyncJob>> asyncJobs() {
		return asyncJobLinkedMap.entrySet();
	}

	public static void main(String[] args) {
		AsyncJobChainImpl asyncJobChain = new AsyncJobChainImpl(null);
		asyncJobChain.job("a", new AsyncJob() {
			@Override
			public JobContext run(JobContext previousJobContext) {
				return null;
			}
		});
		asyncJobChain.job("b", new AsyncJob() {
			@Override
			public JobContext run(JobContext previousJobContext) {
				return null;
			}
		});
		asyncJobChain.job("c", new AsyncJob() {
			@Override
			public JobContext run(JobContext previousJobContext) {
				return null;
			}
		});
		System.out.println("map " + asyncJobChain.asyncJobLinkedMap);
		asyncJobChain.remove("a");
		asyncJobChain.job("a", new AsyncJob() {
			@Override
			public JobContext run(JobContext previousJobContext) {
				return null;
			}
		});
		System.out.println("map1 " + asyncJobChain.asyncJobLinkedMap);
	}
}
