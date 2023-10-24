package io.tapdata.pdk.core.async;


import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.executor.ThreadFactory;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * @author aplomb
 */
public class AsyncUtils {
	public static ThreadPoolExecutorEx createThreadPoolExecutor(String id, int parallelCount, String TAG) {
		return createThreadPoolExecutor(id, parallelCount, null, TAG);
	}
	public static ThreadPoolExecutorEx createThreadPoolExecutor(String id, int parallelCount, ThreadGroup threadGroup, String TAG) {
		ThreadPoolExecutorEx threadPoolExecutor = new ThreadPoolExecutorEx(parallelCount, parallelCount, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<>(100), new ThreadFactory(TAG + "_" + id, threadGroup), (r, executor) -> {
			TapLogger.error(TAG, "Thread is rejected, runnable {} pool {}", r, executor);
		});
		//Don't waste core thread when idle.
		threadPoolExecutor.allowCoreThreadTimeOut(true);
		return threadPoolExecutor;
	}

	static class MyThreadGroup extends ThreadGroup {

		public MyThreadGroup(String name) {
			super(name);
		}
	}
	public static void main(String[] args) {
		new Thread(() -> {
			try(ThreadPoolExecutorEx threadPoolExecutor = createThreadPoolExecutor("aaa", 1, new MyThreadGroup("aaa"), "AAA")) {
				threadPoolExecutor.submitSync(() -> {
					System.out.println("");
					threadPoolExecutor.submit(() -> {
						System.out.println("aa");
						new Thread(() -> {
							System.out.println("aa");
						}).start();
					});

				});
			}
			System.out.println("");
		}).start();


	}
}
