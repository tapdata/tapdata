package io.tapdata.async.master;

import java.util.function.Function;

public interface AsyncQueueWorker extends AsyncWorker {
	String getId();
	int getState();
	AsyncQueueWorker job(AsyncJobChain asyncJobChain);
	AsyncQueueWorker job(AsyncJob asyncJob);
	AsyncQueueWorker job(String id, AsyncJob asyncJob);

	AsyncQueueWorker job(String id, AsyncJob asyncJob, boolean pending);

	AsyncQueueWorker externalJob(String id, Function<JobContext, JobContext> jobContextConsumer);

	AsyncQueueWorker externalJob(String id, Function<JobContext, JobContext> jobContextConsumer, boolean pending);

	AsyncQueueWorker externalJob(String id, AsyncJob asyncJob, Function<JobContext, JobContext> jobContextConsumer);
	AsyncQueueWorker externalJob(String id, AsyncJob asyncJob, Function<JobContext, JobContext> jobContextConsumer, boolean pending);

	AsyncQueueWorker cancelAll();
	AsyncQueueWorker cancel(String id);
	AsyncQueueWorker cancel(String id, boolean immediately);
	String runningJobId();

	AsyncQueueWorker setAsyncJobErrorListener(AsyncJobErrorListener listener);
	AsyncQueueWorker setQueueWorkerStateListener(QueueWorkerStateListener listener);

	AsyncQueueWorker start(JobContext jobContext);
	AsyncQueueWorker start(JobContext jobContext, boolean startOnCurrentThread);

	AsyncQueueWorker start(JobContext jobContext, long delayMilliSeconds, long periodMilliSeconds);

	AsyncQueueWorker stop();

	AsyncQueueWorker threadBefore(Runnable runnable);

	AsyncQueueWorker threadAfter(Runnable runnable);
}
