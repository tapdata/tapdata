package io.tapdata.async.master;

import java.util.function.Function;

public interface AsyncQueueWorker extends AsyncWorker {
	String getId();
	int getState();
	AsyncQueueWorker job(AsyncJobChain asyncJobChain);
	AsyncQueueWorker job(String id, AsyncJob asyncJob);

	AsyncQueueWorker job(String id, AsyncJob asyncJob, boolean pending);

	AsyncQueueWorker externalJob(String id, Function<JobContext, JobContext> jobContextConsumer);

	AsyncQueueWorker externalJob(String id, Function<JobContext, JobContext> jobContextConsumer, boolean pending);

	AsyncQueueWorker cancelAll();
	AsyncQueueWorker cancel(String id);
	AsyncQueueWorker cancel(String id, boolean immediately);
	String runningJobId();

	void setAsyncJobErrorListener(AsyncJobErrorListener listener);
	void setQueueWorkerStateListener(QueueWorkerStateListener listener);

	void start(JobContext jobContext);
	void start(JobContext jobContext, boolean startOnCurrentThread);

	void start(JobContext jobContext, long delayMilliSeconds, long periodMilliSeconds);

	void stop();
}
