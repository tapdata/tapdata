package io.tapdata.async.master;

import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePdkDataNodeEx;
import io.tapdata.modules.api.async.master.AsyncJob;
import io.tapdata.modules.api.async.master.AsyncTools;
import io.tapdata.modules.api.async.master.JobContext;

/**
 * @author aplomb
 */
public abstract class SourceAsyncJob extends AsyncJobImpl {
	protected HazelcastSourcePdkDataNodeEx sourcePdkDataNode;
	@Override
	public JobContext run(JobContext jobContext) {
		sourcePdkDataNode = (HazelcastSourcePdkDataNodeEx) jobContext.getContext();
		return null;
	}

	public abstract JobContext execute(JobContext previousJobContext);
}
