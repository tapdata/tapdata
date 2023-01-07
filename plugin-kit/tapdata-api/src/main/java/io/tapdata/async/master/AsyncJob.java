package io.tapdata.async.master;

import java.util.concurrent.CompletableFuture;

/**
 * @author aplomb
 */
public interface AsyncJob extends JobBase {
	void run(JobContext jobContext, AsyncJobCompleted jobCompleted);
}
