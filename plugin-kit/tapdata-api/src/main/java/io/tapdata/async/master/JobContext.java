package io.tapdata.async.master;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.ClassFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * @author aplomb
 */
public abstract class JobContext {
	public abstract void foreach(int start, int maxCount, Function<Integer, Boolean> function);
	public abstract void foreach(int maxCount, Function<Integer, Boolean> function);
	public abstract <T> void foreach(Iterator<T> iterator, Function<T, Boolean> function);
	public abstract <T> void foreach(Collection<T> collection, Function<T, Boolean> function);

	public abstract void checkJobStoppedOrNot();

	public abstract <K, V> void foreach(Map<K, V> map, Function<Map.Entry<K, V>, Boolean> entryFunction);
	public abstract void runOnce(Runnable runnable);

	private QueueWorker asyncQueueWorker;
	public JobContext asyncQueueWorker(QueueWorker asyncQueueWorker) {
		this.asyncQueueWorker = asyncQueueWorker;
		return this;
	}

	protected final AtomicBoolean stopped = new AtomicBoolean(false);
	protected String stopReason;
	public JobContext stop(String reason) {
		if(stopped.compareAndSet(false, true))
			stopReason = reason;
		return this;
	}

	public JobContext resetStop() {
		stopped.compareAndSet(true, false);
		if(stopReason != null)
			stopReason = null;
		return this;
	}

	protected JobBase asyncJob;
	public JobContext asyncJob(JobBase asyncJob) {
		this.asyncJob = asyncJob;
		return this;
	}
	public static JobContext create() {
		return create(null);
	}
	public static JobContext create(Object result) {
		JobContext jobContext = ClassFactory.create(JobContext.class);
		if(jobContext == null)
			throw new CoreException(9000, "Missing jobContext's implementation");
		return jobContext.result(result);
	}

	protected String id;
	public JobContext id(String id) {
		this.id = id;
		return this;
	}

	protected Object result;
	public JobContext result(Object result) {
		this.result = result;
		return this;
	}

	protected Object context;
	public JobContext context(Object context) {
		this.context = context;
		return this;
	}

	protected String jumpToId;
	public JobContext jumpToId(String jumpToId) {
		this.jumpToId = jumpToId;
		return this;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public <T> T getResult(Class<T> tClass) {
		return (T) result;
	}
	public Object getResult() {
		return result;
	}

	public String getJumpToId() {
		return jumpToId;
	}

	public void setJumpToId(String jumpToId) {
		this.jumpToId = jumpToId;
	}

	public void setResult(Object result) {
		this.result = result;
	}

	public <T> T getContext(Class<T> tClass) {
		return (T) context;
	}

	public Object getContext() {
		return context;
	}

	public void setContext(Object context) {
		this.context = context;
	}

	public JobBase getAsyncJob() {
		return asyncJob;
	}

	public void setAsyncJob(Job asyncJob) {
		this.asyncJob = asyncJob;
	}

	public boolean isStopped() {
		return stopped.get();
	}

	public String getStopReason() {
		return stopReason;
	}

	public void setStopReason(String stopReason) {
		this.stopReason = stopReason;
	}

	public QueueWorker getAsyncQueueWorker() {
		return asyncQueueWorker;
	}
}
