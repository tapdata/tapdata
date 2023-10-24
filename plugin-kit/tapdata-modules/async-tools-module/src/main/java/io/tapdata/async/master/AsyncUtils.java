package io.tapdata.async.master;

import io.tapdata.entity.logger.TapLogger;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author aplomb
 */
public class AsyncUtils {
	public static ThreadPoolExecutor createThreadPoolExecutor(String id, int parallelCount, String TAG) {
		ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(parallelCount, parallelCount, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<>(100), new io.tapdata.pdk.core.executor.ThreadFactory(TAG + "_" + id), (r, executor) -> {
			TapLogger.error(TAG, "Thread is rejected, runnable {} pool {}", r, executor);
		});
		//Don't waste core thread when idle.
		threadPoolExecutor.allowCoreThreadTimeOut(true);
		return threadPoolExecutor;
	}
}
