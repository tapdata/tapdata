package io.tapdata.async.master;

import java.util.function.Function;

public interface QueueWorker extends AsyncWorker {
	String getId();
	int getState();
	QueueWorker job(JobChain asyncJobChain);
	QueueWorker job(Job asyncJob);
	QueueWorker job(String id, Job asyncJob);

	QueueWorker job(String id, Job asyncJob, boolean pending);

	QueueWorker finished();

	QueueWorker finished(String id, Job job);

	QueueWorker finished(Job job);

	QueueWorker externalJob(String id, Function<JobContext, JobContext> jobContextConsumer);

	QueueWorker externalJob(String id, Function<JobContext, JobContext> jobContextConsumer, boolean pending);

	QueueWorker externalJob(String id, Job asyncJob, Function<JobContext, JobContext> jobContextConsumer);
	QueueWorker externalJob(String id, Job asyncJob, Function<JobContext, JobContext> jobContextConsumer, boolean pending);

	QueueWorker cancelAll();
	QueueWorker cancel(String id);
	QueueWorker cancel(String id, boolean immediately);
	QueueWorker setAsyncJobErrorListener(JobErrorListener listener);
	QueueWorker setQueueWorkerStateListener(QueueWorkerStateListener listener);

	QueueWorker start(JobContext jobContext);
	QueueWorker start(JobContext jobContext, boolean startOnCurrentThread);

	QueueWorker stop();

	QueueWorker threadBefore(Runnable runnable);

	QueueWorker threadAfter(Runnable runnable);

	QueueWorker asyncJob(AsyncJob asyncJob);
	QueueWorker asyncJob(AsyncJob asyncJob, boolean pending);
	QueueWorker asyncJob(String id, AsyncJob asyncJob);
	QueueWorker asyncJob(String id, AsyncJob asyncJob, boolean pending);
}
