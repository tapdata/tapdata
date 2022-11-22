package io.tapdata.modules.api.async.master;

public interface AsyncQueueWorker extends AsyncWorker {
	String getId();
	int getState();
	AsyncQueueWorker add(AsyncJobChain asyncJobChain);
	AsyncQueueWorker add(String id, AsyncJob asyncJob);
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
