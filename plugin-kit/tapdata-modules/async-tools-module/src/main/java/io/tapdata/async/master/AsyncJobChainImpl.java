package io.tapdata.async.master;

import io.tapdata.modules.api.async.master.AsyncJob;
import io.tapdata.modules.api.async.master.AsyncJobChain;
import io.tapdata.modules.api.async.master.AsyncTools;
import io.tapdata.modules.api.async.master.JobContext;

import java.util.*;

/**
 * @author aplomb
 */
public class AsyncJobChainImpl implements AsyncJobChain {
	final Map<String, AsyncJob> asyncJobLinkedMap = Collections.synchronizedMap(new LinkedHashMap<>());

	public Map<String, AsyncJob> clone() {
		return Collections.synchronizedMap(new LinkedHashMap<>(asyncJobLinkedMap));
	}

	@Override
	public AsyncJobChain add(Map.Entry<String, AsyncJob> asyncJobChain) {

		return null;
	}

	@Override
	public AsyncJobChain add(String id, AsyncJob asyncJob) {
		asyncJobLinkedMap.put(id, asyncJob);
		return this;
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
		asyncJobChain.add("a", new SourceAsyncJob() {
			@Override
			public JobContext execute(JobContext previousJobContext) {
				return null;
			}
		});
		asyncJobChain.add("b", new SourceAsyncJob() {
			@Override
			public JobContext execute(JobContext previousJobContext) {
				return null;
			}
		});
		asyncJobChain.add("c", new SourceAsyncJob() {
			@Override
			public JobContext execute(JobContext previousJobContext) {
				return null;
			}
		});
		System.out.println("map " + asyncJobChain.asyncJobLinkedMap);
		asyncJobChain.remove("a");
		asyncJobChain.add("a", new SourceAsyncJob() {
			@Override
			public JobContext execute(JobContext previousJobContext) {
				return null;
			}
		});
		System.out.println("map1 " + asyncJobChain.asyncJobLinkedMap);
	}
}
