package io.tapdata.observable.metric.throttle;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.observable.metric.handler.HandlerUtil;
import io.tapdata.observable.metric.handler.RandomSampleEventHandler;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic interleaving and concurrent regression tests for AC-32 (shared batch count race fix).
 */
class FullBatchConcurrencyRegressionTest {

    @Test
    void deterministicInterleavingNeverCorruptsBatchSizeAcrossThreads() throws Exception {
        CountDownLatch batchAEnteredUnitConversion = new CountDownLatch(1);
        CountDownLatch batchBCompleted = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger(0);

        RandomSampleEventHandler handler = new RandomSampleEventHandler(0.5) {
            @Override
            protected void unitConversion(HandlerUtil.EventTypeRecorder recorder, long sizeOfSampleListByte, long batchSize) {
                int callIndex = calls.incrementAndGet();
                if (callIndex == 1 && batchSize == 100L) {
                    batchAEnteredUnitConversion.countDown();
                    try {
                        assertTrue(batchBCompleted.await(5, TimeUnit.SECONDS), "Batch B did not complete in time");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
                super.unitConversion(recorder, sizeOfSampleListByte, batchSize);
            }
        };

        List<TapEvent> batchA = buildEvents(100, 4_000L);
        List<TapEvent> batchB = buildEvents(20, 800L);
        HandlerUtil.EventTypeRecorder recorderA = new HandlerUtil.EventTypeRecorder();
        HandlerUtil.EventTypeRecorder recorderB = new HandlerUtil.EventTypeRecorder();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> futureA = pool.submit(() -> handler.sampleMemoryTapEvent(recorderA, batchA, e -> (TapEvent) e));
            assertTrue(batchAEnteredUnitConversion.await(5, TimeUnit.SECONDS));

            Future<?> futureB = pool.submit(() -> {
                handler.sampleMemoryTapEvent(recorderB, batchB, e -> (TapEvent) e);
                batchBCompleted.countDown();
            });

            futureA.get(5, TimeUnit.SECONDS);
            futureB.get(5, TimeUnit.SECONDS);

            // Legacy /4 formula: A = 4000 * 100 / 4 = 100,000 B; B = 800 * 20 / 4 = 4,000 B
            assertEquals(100_000L, recorderA.getMemorySize());
            assertEquals("B", recorderA.getMemoryUtil());
            assertEquals(4_000L, recorderB.getMemorySize());
            assertEquals("B", recorderB.getMemoryUtil());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void concurrentMultiTableAndProcessorInterleavingMatchesSequentialTotals() throws Exception {
        int threads = 8;
        int iterations = 300;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int batchCount = (t % 4 == 0) ? 1 : (t % 4 == 1 ? 20 : (t % 4 == 2 ? 73 : 150));
            final long firstSize = 400L * (t + 1);
            final long expectedMemory = (firstSize * batchCount) / 4L;
            tasks.add(() -> {
                assertTrue(startGate.await(5, TimeUnit.SECONDS));
                for (int i = 0; i < iterations; i++) {
                    List<TapEvent> events = buildEvents(batchCount, firstSize);
                    HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapEvent(events);
                    assertEquals(batchCount, recorder.getInsertTotal());
                    assertEquals(expectedMemory, recorder.getMemorySize());
                }
                return null;
            });
        }

        try {
            List<Future<Void>> futures = new ArrayList<>();
            for (Callable<Void> task : tasks) {
                futures.add(pool.submit(task));
            }
            startGate.countDown();
            for (Future<Void> future : futures) {
                future.get(15, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void emptyListSafeAndTotalSizeFieldRemoved() {
        RandomSampleEventHandler handler = new RandomSampleEventHandler(0.5);
        assertEquals(0L, handler.sampleMemoryTapEvent(List.of(), e -> (TapEvent) e));
        assertDoesNotThrow(() -> HandlerUtil.sampleMemoryToTapEvent(new ArrayList<>()));
        assertThrows(NoSuchFieldException.class, () -> RandomSampleEventHandler.class.getDeclaredField("totalSize"));
    }

    private static List<TapEvent> buildEvents(int count, long firstMemorySize) {
        List<TapEvent> events = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            TapInsertRecordEvent event = new TapInsertRecordEvent();
            event.table("t_" + count);
            event.referenceTime(1_000L + i);
            Map<String, Object> after = new HashMap<>();
            after.put("id", i);
            event.after(after);
            event.setMemorySize(firstMemorySize);
            events.add(event);
        }
        return events;
    }
}
