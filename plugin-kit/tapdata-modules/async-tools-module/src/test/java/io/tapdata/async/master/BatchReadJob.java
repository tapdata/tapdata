package io.tapdata.async.master;

import io.tapdata.modules.api.async.master.AsyncJobClass;

import static io.tapdata.entity.simplify.TapSimplify.*;

/**
 * @author aplomb
 */
@AsyncJobClass("batchRead")
public class BatchReadJob implements Job {

	@Override
	public JobContext run(JobContext jobContext) {
		return JobContext.create(list(insertRecordEvent(map(entry("a", 1)), "table")));
	}
}
