package io.tapdata.async.master;

import io.tapdata.modules.api.async.master.AsyncJob;
import io.tapdata.modules.api.async.master.AsyncJobChain;
import io.tapdata.modules.api.async.master.JobContext;

import java.util.*;
import java.util.function.Function;

/**
 * @author aplomb
 */
public class AsyncJobChainImpl implements AsyncJobChain {
	final Map<String, AsyncJob> asyncJobLinkedMap = Collections.synchronizedMap(new LinkedHashMap<>());

	public Map<String, AsyncJob> clone() {
		return Collections.synchronizedMap(new LinkedHashMap<>(asyncJobLinkedMap));
	}

	@Override
	public AsyncJobChain job(Map.Entry<String, AsyncJob> entry) {
		asyncJobLinkedMap.put(entry.getKey(), entry.getValue());
		return this;
	}

	@Override
	public AsyncJobChain job(String id, AsyncJob asyncJob) {
		asyncJobLinkedMap.put(id, asyncJob);
		return this;
	}

	@Override
	public AsyncJobChain externalJob(String id, Function<JobContext, JobContext> jobContextConsumer) {
		return null;
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
		AsyncJobChainImpl asyncJobChain = new AsyncJobChainImpl();
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
