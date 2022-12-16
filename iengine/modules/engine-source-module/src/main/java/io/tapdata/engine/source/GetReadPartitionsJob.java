package io.tapdata.engine.source;

import io.tapdata.async.master.JobContext;
import io.tapdata.modules.api.async.master.AsyncJobClass;

/**
 * @author aplomb
 */
@AsyncJobClass("getReadPartitions")
public class GetReadPartitionsJob extends SourceAsyncJob {
	@Override
	public JobContext execute(JobContext previousJobContext) {

		return null;
	}
}
