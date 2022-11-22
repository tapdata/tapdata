package io.tapdata.async.master;

import io.tapdata.modules.api.async.master.AsyncJob;
import io.tapdata.modules.api.async.master.AsyncJobClass;
import io.tapdata.modules.api.async.master.AsyncTools;
import io.tapdata.modules.api.async.master.JobContext;

/**
 * @author aplomb
 */
@AsyncJobClass("batchRead")
public class BatchReadJob extends SourceAsyncJob {

	@Override
	public JobContext execute(JobContext previousJobContext) {

		return null;
	}
}
