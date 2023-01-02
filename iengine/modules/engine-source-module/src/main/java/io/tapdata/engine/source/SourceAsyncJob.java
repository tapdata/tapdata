package io.tapdata.engine.source;

import io.tapdata.async.master.AsyncJob;
import io.tapdata.async.master.JobContext;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.PDKSourceContext;
import io.tapdata.observable.logging.ObsLogger;

/**
 * @author aplomb
 */
public abstract class SourceAsyncJob implements AsyncJob {
	protected PDKSourceContext pdkSourceContext;
	protected ObsLogger obsLogger;
	@Override
	public JobContext run(JobContext jobContext) {
		pdkSourceContext = (PDKSourceContext) jobContext.getContext();
		obsLogger = pdkSourceContext.getSourcePdkDataNode().getObsLogger();
		return execute(jobContext);
	}

	public abstract JobContext execute(JobContext previousJobContext);
}
