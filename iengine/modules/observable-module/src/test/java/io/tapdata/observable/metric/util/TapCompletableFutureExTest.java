package io.tapdata.observable.metric.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-06-28 14:13
 **/
class TapCompletableFutureExTest {
	@Test
	@DisplayName("Method create test")
	void testCreate() {
		TapCompletableFutureEx tapCompletableFutureEx = TapCompletableFutureEx.create(10000, 1000);

		assertEquals(10000, tapCompletableFutureEx.getQueueSize());
		assertEquals(1000, tapCompletableFutureEx.getJoinWatermark());
		Object running = ReflectionTestUtils.getField(tapCompletableFutureEx, "running");
		assertNotNull(running);
		assertFalse(((AtomicBoolean) running).get());
		Object completableFutureQueue = ReflectionTestUtils.getField(tapCompletableFutureEx, "completableFutureQueue");
		assertNotNull(completableFutureQueue);
		assertEquals(10000, ((LinkedBlockingQueue<?>) completableFutureQueue).remainingCapacity());
		Object joinThreadPool = ReflectionTestUtils.getField(tapCompletableFutureEx, "joinThreadPool");
		assertNotNull(joinThreadPool);
		assertInstanceOf(ThreadPoolExecutor.class, joinThreadPool);
		assertEquals(1, ((ThreadPoolExecutor) joinThreadPool).getCorePoolSize());
		assertEquals(1, ((ThreadPoolExecutor) joinThreadPool).getMaximumPoolSize());

		tapCompletableFutureEx = TapCompletableFutureEx.create(0, 0);
		assertEquals(TapCompletableFutureEx.DEFAULT_QUEUE_SIZE, tapCompletableFutureEx.getQueueSize());
		assertEquals(TapCompletableFutureEx.DEFAULT_JOIN_WATERMARK, tapCompletableFutureEx.getJoinWatermark());
	}

	@Test
	@DisplayName("Method start test")
	void testStart() {
		TapCompletableFutureEx tapCompletableFutureEx = TapCompletableFutureEx.create(10000, 1000).start();

		Object running = ReflectionTestUtils.getField(tapCompletableFutureEx, "running");
		assertNotNull(running);
		assertTrue(((AtomicBoolean) running).get());
		Object joinThreadPool = ReflectionTestUtils.getField(tapCompletableFutureEx, "joinThreadPool");
		assertNotNull(joinThreadPool);
		assertDoesNotThrow(() -> TimeUnit.MILLISECONDS.sleep(5L));
		assertEquals(1, ((ThreadPoolExecutor) joinThreadPool).getActiveCount());
	}

	@Test
	@DisplayName("Method runAsync test, single thread")
	void testRunAsyncSingleThread() {
		TapCompletableFutureEx tapCompletableFutureEx = TapCompletableFutureEx.create(1000, 1000).start();
		AtomicBoolean run = new AtomicBoolean(false);
		tapCompletableFutureEx.runAsync(() -> {
			try {
				TimeUnit.SECONDS.sleep(1L);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			run.set(true);
		});
		tapCompletableFutureEx.stop(1L, TimeUnit.MINUTES);
		assertTrue(run.get());
	}

	@Test
	@DisplayName("Method runAsync test, multi thread")
	void testRunAsyncMultiThread() {
		int threadNum = 4, threadBatch = 1000000;
		long start = System.currentTimeMillis();
		List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
		TapCompletableFutureEx tapCompletableFutureEx = TapCompletableFutureEx.create(Integer.MAX_VALUE, threadBatch).start();
		AtomicInteger counter = new AtomicInteger();
		for (int i = 0; i < threadNum; i++) {
			completableFutures.add(
					CompletableFuture.runAsync(() -> {
						for (int j = 0; j < threadBatch; j++) {
							assertDoesNotThrow(() -> tapCompletableFutureEx.runAsync(counter::incrementAndGet));
						}
					})
			);
		}

		assertDoesNotThrow(() -> CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0])).join());
		assertDoesNotThrow(() -> tapCompletableFutureEx.stop(5L, TimeUnit.MINUTES));
		assertEquals(threadNum * threadBatch, counter.get());
		long cost = System.currentTimeMillis() - start;
		BigDecimal qps;
		if ((cost / 1000) == 0) {
			qps = BigDecimal.valueOf(threadBatch * threadNum);
		} else {
			qps = BigDecimal.valueOf(threadNum * threadBatch).divide(BigDecimal.valueOf(cost / 1000), 2, RoundingMode.HALF_UP);
		}
		System.out.println("cost: " + cost + ", qps: " + qps);
	}
	@Test
	void testThenRunMain(){
		TapCompletableFutureEx tapCompletableFutureEx = mock(TapCompletableFutureEx.class);
		CompletableFuture<Void> future = mock(CompletableFuture.class);
		when(future.thenRun(any())).thenReturn(future);
		ReflectionTestUtils.setField(tapCompletableFutureEx, "running", new AtomicBoolean(true));
		ReflectionTestUtils.setField(tapCompletableFutureEx, "completableFutureQueue", new LinkedBlockingQueue<CompletableFuture<Void>>());
		ReflectionTestUtils.setField(tapCompletableFutureEx, "lastFuture", future);
		doCallRealMethod().when(tapCompletableFutureEx).thenRun(any());
		tapCompletableFutureEx.thenRun(() -> {});
		verify(future,times(1)).thenRun(any());
	}
	@Test
	void testThenRunError(){
		TapCompletableFutureEx tapCompletableFutureEx = mock(TapCompletableFutureEx.class);
		ReflectionTestUtils.setField(tapCompletableFutureEx, "running", new AtomicBoolean(false));
		ReflectionTestUtils.setField(tapCompletableFutureEx, "firstTime", new AtomicBoolean(true));
		doCallRealMethod().when(tapCompletableFutureEx).thenRun(any());
		Assertions.assertThrows(IllegalStateException.class, () -> tapCompletableFutureEx.thenRun(() -> {}));
	}
}