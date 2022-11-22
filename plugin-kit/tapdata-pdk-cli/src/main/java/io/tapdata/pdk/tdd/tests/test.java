package io.tapdata.pdk.tdd.tests;

import io.tapdata.entity.logger.TapLogger;

import java.text.SimpleDateFormat;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class test {
	static Thread thread;
	public static void main(String... args) throws Throwable {
		ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<>(100), new io.tapdata.pdk.core.executor.ThreadFactory("aaa"), (r, executor) -> {
			TapLogger.error("TAG", "Thread is rejected, runnable {} pool {}", r, executor);
		});
		Runnable runnable = new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				threadPoolExecutor.execute(this);
			}
		};
		threadPoolExecutor.execute(runnable);
    }
}
