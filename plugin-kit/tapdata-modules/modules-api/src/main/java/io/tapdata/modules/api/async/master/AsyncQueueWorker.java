package io.tapdata.modules.api.async.master;

public interface AsyncQueueWorker extends AsyncWorker {
	String getId();
	AsyncQueueWorker add(AsyncJobChain asyncJobChain);
	AsyncQueueWorker add(String id, AsyncJob asyncJob);
	AsyncQueueWorker cancelAll();
	AsyncQueueWorker cancel(String id);
	AsyncQueueWorker cancel(String id, boolean immediately);
	String runningId();

	<T> void start(JobContext<T> jobContext);
	<T> void start(JobContext<T> jobContext, boolean startOnCurrentThread);
}
