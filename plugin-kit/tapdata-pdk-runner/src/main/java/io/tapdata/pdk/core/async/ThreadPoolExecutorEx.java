package io.tapdata.pdk.core.async;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.concurrent.*;

/**
 * @author aplomb
 */
public class ThreadPoolExecutorEx extends ThreadPoolExecutor implements AutoCloseable {
	public interface BeforeExecute {
		void beforeExecute(Thread t, Runnable r);
	}
	public interface AfterExecute {
		void afterExecute(Runnable r, Throwable t);
	}

	private BeforeExecute beforeExecute;
	public ThreadPoolExecutorEx beforeExecute(BeforeExecute beforeExecute) {
		this.beforeExecute = beforeExecute;
		return this;
	}

	private AfterExecute afterExecute;
	public ThreadPoolExecutorEx afterExecute(AfterExecute afterExecute) {
		this.afterExecute = afterExecute;
		return this;
	}

	public ThreadPoolExecutorEx(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
	}
	protected void beforeExecute(Thread t, Runnable r) {
		if(beforeExecute != null)
			beforeExecute.beforeExecute(t, r);

		super.beforeExecute(t, r);
	}
	protected void afterExecute(Runnable r, Throwable t) {
		if(afterExecute != null)
			afterExecute.afterExecute(r, t);

		super.afterExecute(r, t);
	}

	public <T> T submitSync(Callable<T> task) {
		try {
			return submit(task).get();
		} catch (InterruptedException e) {
			throw new CoreException(PDKRunnerErrorCodes.SUBMIT_SYNC_CALLABLE_FAILED, e, "Submit sync task {} failed, {}", task, InstanceFactory.instance(TapUtils.class).getStackTrace(e));
		} catch (ExecutionException e) {
			Throwable throwable = e.getCause();
			if(throwable != null)
				throw new CoreException(PDKRunnerErrorCodes.SUBMIT_SYNC_CALLABLE_FAILED, throwable, "Submit sync task {} failed, {}", task, InstanceFactory.instance(TapUtils.class).getStackTrace(throwable));
			throw new CoreException(PDKRunnerErrorCodes.SUBMIT_SYNC_CALLABLE_FAILED, e, "Submit sync task {} failed, {}", task, InstanceFactory.instance(TapUtils.class).getStackTrace(e));
		}
	}
	public void submitSync(CommonUtils.AnyError task) {
		try {
			submit(() -> {
				try {
					task.run();
				} catch (Throwable throwable) {
					throw new RuntimeException(throwable);
				}
			}).get();
		} catch (ExecutionException e) {
			Throwable throwable = e.getCause();
			if(throwable != null)
				throw new CoreException(PDKRunnerErrorCodes.SUBMIT_SYNC_RUNNABLE_FAILED, throwable, "Submit sync task {} failed, {}", task, InstanceFactory.instance(TapUtils.class).getStackTrace(throwable));
			throw new CoreException(PDKRunnerErrorCodes.SUBMIT_SYNC_RUNNABLE_FAILED, e, "Submit sync task {} failed, {}", task, InstanceFactory.instance(TapUtils.class).getStackTrace(e));
		} catch (Throwable e) {
			throw new CoreException(PDKRunnerErrorCodes.SUBMIT_SYNC_RUNNABLE_FAILED, e, "Submit sync task {} failed, {}", task, InstanceFactory.instance(TapUtils.class).getStackTrace(e));
		}
	}

	@Override
	public void close() {
//		ThreadFactory threadFactory = getThreadFactory();
//		if(threadFactory instanceof io.tapdata.pdk.core.executor.ThreadFactory) {
//			ThreadGroup threadGroup = ((io.tapdata.pdk.core.executor.ThreadFactory) threadFactory).getThreadGroup();
//			if(threadGroup != null && !threadGroup.isDestroyed())
//				Thread.currentThread().getThreadGroup().destroy();
//		}
		shutdown();
	}
}
