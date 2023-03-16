package com.tapdata.constant;

import com.tapdata.entity.Job;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class ExecutorUtil {

	private Logger logger = LogManager.getLogger(ExecutorUtil.class);
	public static final int DEFAULT_LIST_SIZE = 1000;

	public static void shutdown(ExecutorService executorService, long await, TimeUnit timeUnit) {
		if (null == executorService) {
			return;
		}
		await = await < 0 ? 0 : await;
		executorService.shutdown();
		try {
			if (!executorService.awaitTermination(await, timeUnit)) {
				executorService.shutdownNow();
			}
		} catch (InterruptedException e) {
			executorService.shutdownNow();
		}
	}

	public void shutdown(ExecutorService executorService, Supplier<?> shutdownNow, long await, TimeUnit timeUnit) {
		if (executorService != null) {
			await = await < 0 ? 0 : await;
			executorService.shutdown();
			try {
				if (!executorService.awaitTermination(await, timeUnit)) {
					shutdownNow.get();
				}
			} catch (InterruptedException e) {
				shutdownNow.get();
			}
		}
	}

	private Thread inQueueThread;
	private ExecutorService executorService;
	private AtomicBoolean finish = new AtomicBoolean(false);
	private AtomicInteger progress;
	private CountDownLatch countDownLatch;

	public <T, K> void queueMultithreading(List<T> list, Predicate<T> predicate, Consumer<T> consumer, String threadName) {
		queueMultithreading(getSuggestThreadCount(), DEFAULT_LIST_SIZE, list, predicate, consumer, threadName, null, null, null);
	}

	public <T, K> void queueMultithreading(List<T> list, Predicate<T> predicate, Consumer<T> consumer, String threadName, K k, Predicate<K> stop, Job job) {
		queueMultithreading(getSuggestThreadCount(), DEFAULT_LIST_SIZE, list, predicate, consumer, threadName, k, stop, job);
	}

	public <T, K> void queueMultithreading(int threadCount, List<T> list, Predicate<T> predicate, Consumer<T> consumer, String threadName, K k, Predicate<K> stop, Job job) {
		queueMultithreading(threadCount, DEFAULT_LIST_SIZE, list, predicate, consumer, threadName, k, stop, job);
	}

	public <T, K> void queueMultithreading(int threadCount, int queueSize, List<T> list, Predicate<T> predicate, Consumer<T> consumer, String threadName, K k, Predicate<K> stop, Job job) {
		executorService = new ThreadPoolExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
		finish.set(false);
		progress = new AtomicInteger(0);
		if (CollectionUtils.isEmpty(list)) {
			return;
		}
		LinkedBlockingQueue<T> linkedBlockingQueue = new LinkedBlockingQueue<>(queueSize);
		inQueueThread = new Thread(() -> {
			for (T t : list) {
				if (Thread.currentThread().isInterrupted()) {
					break;
				}
				while (true) {
					try {
						boolean offer = linkedBlockingQueue.offer(t, 500, TimeUnit.MILLISECONDS);
						if (offer) {
							break;
						}
					} catch (InterruptedException e) {
						break;
					}
				}
			}
			finish.set(true);
		});
		inQueueThread.start();

		countDownLatch = new CountDownLatch(threadCount);
		IntStream.range(0, threadCount).forEach(threadIndex -> {
			executorService.execute(() -> {
				try {
					if (job != null) {
						Log4jUtil.setThreadContext(job);
					}
					if (StringUtils.isNotBlank(threadName)) {
						Thread.currentThread().setName(formatThreadName(threadName) + "-" + threadIndex);
					}
					while (true) {
						if (Thread.currentThread().isInterrupted()) {
							break;
						}
						if (finish.get() && linkedBlockingQueue.isEmpty()) {
							break;
						}
						try {
							T t = linkedBlockingQueue.poll(500, TimeUnit.MILLISECONDS);
							if (t == null) {
								continue;
							}
							if (predicate != null && !predicate.test(t)) {
								printProgress(list, threadName);
								continue;
							}
							try {
								consumer.accept(t);
							} catch (InterruptExecutorException e) {
								break;
							}
							printProgress(list, threadName);
						} catch (InterruptedException e) {
							break;
						}
					}
				} finally {
					countDownLatch.countDown();
				}
			});
		});
		waitForFinishAndReleaseThread(k, stop);
	}

	private <T> void printProgress(List<T> list, String threadName) {
		int progressInt = progress.incrementAndGet();
		if (progressInt % ConnectorConstant.LOOP_BATCH_SIZE == 0) {
			logger.info(threadName + " progress: " + progressInt + "/" + list.size());
		}
	}

	public boolean isFinish() {
		return finish.get();
	}

	public int getProgress() {
		if (progress == null) {
			return 0;
		}
		return progress.get();
	}

	public void stopQueueThread() {
		try {
			if (inQueueThread != null) {
				inQueueThread.interrupt();
			}
			shutdown(executorService, 10L, TimeUnit.SECONDS);
		} catch (Exception ignore) {
		}
	}

	public <K> void waitForFinishAndReleaseThread(K k, Predicate<K> stop) {
		try {
			while (true) {
				if (Thread.currentThread().isInterrupted()) {
					break;
				}
				if (k != null && stop != null && stop.test(k)) {
					break;
				}
				if (countDownLatch.await(500, TimeUnit.MILLISECONDS)) {
					break;
				}
			}
		} catch (Exception ignore) {
		} finally {
			stopQueueThread();
		}
	}

	public int getSuggestThreadCount() {
		int availableProcessors = Runtime.getRuntime().availableProcessors();
		return availableProcessors <= 2 ? availableProcessors : availableProcessors / 2;
	}

	private String formatThreadName(String threadName) {
		return threadName.replaceAll("\\s", "-").toUpperCase();
	}

	public static class InterruptExecutorException extends RuntimeException {
		public InterruptExecutorException() {
			super();
		}

		public InterruptExecutorException(String message) {
			super(message);
		}

		public InterruptExecutorException(String message, Throwable cause) {
			super(message, cause);
		}

		public InterruptExecutorException(Throwable cause) {
			super(cause);
		}

		protected InterruptExecutorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
			super(message, cause, enableSuppression, writableStackTrace);
		}
	}
}
