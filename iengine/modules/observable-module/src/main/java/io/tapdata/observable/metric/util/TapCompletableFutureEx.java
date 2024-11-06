package io.tapdata.observable.metric.util;

import com.google.common.collect.Queues;
import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author samuel
 * @Description Asynchronous task executor based on CompletableFuture, submit Runnable through runAsync method.
 * It will automatically execute asynchronously, and stop through stop method.
 * This utility method does not guarantee execution order, but guarantees that all submitted tasks are executed before stopping.
 * Usage example
 * <pre>
 *     TapCompletableFutureEx tapCompletableFutureEx = TapCompletableFutureEx.create(1000, 1000).start();
 *     tapCompletableFutureEx.runAsync(() -> {
 *     	// do something
 *     });
 * 	   tapCompletableFutureEx.runAsync(() -> {
 *     	// do something
 *     });
 *     tapCompletableFutureEx.stop(1L, TimeUnit.SECONDS);
 * </pre>
 * @create 2024-06-28 12:01
 **/
public class TapCompletableFutureEx {
	public static final String TAG = TapCompletableFutureEx.class.getSimpleName();
	public static final String STOP_JOIN_CHECKER_THREAD_NAME = String.join("-", TAG, "stop", "join", "checker");
	public static final String RUNNING_JOIN_CHECKER_THREAD_NAME = String.join("-", TAG, "running", "join", "checker");
	public static final int DEFAULT_QUEUE_SIZE = 1000;
	public static final int DEFAULT_JOIN_WATERMARK = 1000;

	protected final AtomicBoolean running;
	protected final int queueSize;
	protected final LinkedBlockingQueue<CompletableFuture<Void>> completableFutureQueue;
	protected final int joinWatermark;
	protected ExecutorService joinThreadPool;
	private final int[] joinLock = new int[0];
	protected final AtomicBoolean firstTime = new AtomicBoolean(true);
	private CompletableFuture<Void> lastFuture;

	protected TapCompletableFutureEx(int queueSize, int joinWatermark, String threadName) {
		this.running = new AtomicBoolean(false);
		this.queueSize = queueSize <= 0 ? DEFAULT_QUEUE_SIZE : queueSize;
		this.joinWatermark = joinWatermark <= 0 ? DEFAULT_JOIN_WATERMARK : joinWatermark;
		this.completableFutureQueue = new LinkedBlockingQueue<>(this.queueSize);
		this.joinThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
				r -> new Thread(r, threadName));
		this.lastFuture = CompletableFuture.runAsync(() -> {
		});
	}

	public static TapCompletableFutureEx create(int queueSize, int joinWatermark) {
		return new TapCompletableFutureEx(queueSize, joinWatermark, RUNNING_JOIN_CHECKER_THREAD_NAME);
	}

	public TapCompletableFutureEx start() {
		this.running.compareAndSet(false, true);
		this.joinThreadPool.execute(() -> {
			while (running.get()) {
				checkJoin();
			}
		});
		return this;
	}

	public void runAsync(Runnable runnable) {
		if (!running.get() && firstTime.compareAndSet(true, false)) {
			throw new IllegalStateException("TapCompletableFutureEx not started");
		}
		enqueue(CompletableFuture.runAsync(runnable));
	}

	public void thenRun(Runnable runnable) {
		if (!running.get() && firstTime.compareAndSet(true, false)) {
			throw new IllegalStateException("TapCompletableFutureEx not started");
		}
		lastFuture = lastFuture.thenRun(runnable);
		enqueue(lastFuture);
	}

	private void enqueue(CompletableFuture<Void> completableFuture) {
		while (true) {
			boolean isBreak = false;
			try {
				if (completableFutureQueue.offer(completableFuture, 10L, TimeUnit.MILLISECONDS)) {
					isBreak = true;
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				isBreak = true;
			}
			if (isBreak) {
				break;
			}
		}
	}

	protected void checkJoin() {
		synchronized (joinLock) {
			List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
			try {
				Queues.drain(completableFutureQueue, completableFutures, joinWatermark, 1L, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			if (CollectionUtils.isEmpty(completableFutures)) {
				return;
			}
			CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0])).join();
		}
	}

	public void stop(long timeout, TimeUnit timeUnit) {
		stopPrivate(timeout, timeUnit, STOP_JOIN_CHECKER_THREAD_NAME);
	}

	protected void stopPrivate(long timeout, TimeUnit timeUnit, String threadName) {
		running.compareAndSet(true, false);
		joinThreadPool.shutdownNow();
		CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
			Thread.currentThread().setName(threadName);
			stopCheckJoin();
		});
		waitDone(future, timeout, timeUnit);
		this.lastFuture = null;
		Thread.currentThread().interrupt();
	}

	protected void stopCheckJoin() {
		synchronized (joinLock) {
			while (true) {
				boolean isBreak = false;
				List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
				try {
					Queues.drain(completableFutureQueue, completableFutureList, completableFutureQueue.size(), 1L, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					isBreak = true;
				}
				if (!completableFutureList.isEmpty()) {
					CompletableFuture.allOf(completableFutureList.toArray(new CompletableFuture[0])).join();
				} else {
					isBreak = true;
				}
				try {
					TimeUnit.MILLISECONDS.sleep(100L);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					isBreak = true;
				}
				if (isBreak) {
					break;
				}
			}
		}
	}

	private void waitDone(CompletableFuture<Void> future, long timeout, TimeUnit timeUnit) {
		long timeoutMs = timeUnit.toMillis(timeout);
		long s = System.currentTimeMillis();
		while (true) {
			long cost = System.currentTimeMillis() - s;
			boolean futureDone = future.isDone();
			boolean isBreak = cost > timeoutMs || futureDone;
			try {
				TimeUnit.MILLISECONDS.sleep(1L);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				isBreak = true;
			}
			if (isBreak) {
				TapLogger.info(TAG, "Stop done, timeout: {} ms, cost time: {} ms, stop join checker isDone: {}", timeoutMs, cost, futureDone);
				future.cancel(true);
				break;
			}
		}
	}

	public int getQueueSize() {
		return queueSize;
	}

	public int getJoinWatermark() {
		return joinWatermark;
	}
}
