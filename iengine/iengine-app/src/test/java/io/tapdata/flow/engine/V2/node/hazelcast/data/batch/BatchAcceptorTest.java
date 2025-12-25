package io.tapdata.flow.engine.V2.node.hazelcast.data.batch;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.consumer.TapStreamReadConsumer;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.mockito.Mockito.*;

class BatchAcceptorTest {
    ObsLogger obsLogger;
    BatchAcceptor.ValueGetter<Integer> batchSizeGetter;
    Supplier<Long> delayMsGetter;
    Predicate<Boolean> isAlive;
    TapStreamReadConsumer<List<TapEvent>, Object> consumer;

    @BeforeEach
    void beforeEach() {
        obsLogger = mock(ObsLogger.class);
        batchSizeGetter = () -> 10;
        delayMsGetter = () -> 100L;
        isAlive = b -> true;
        consumer = mock(TapStreamReadConsumer.class);
        doNothing().when(obsLogger).debug(anyString(), anyString());
    }

    @Nested
    class GetDelayMsTest {
        @Test
        void testGetDelayMs() {
            BatchAcceptor.ValueGetter<Integer> batchSizeGetter = () -> 10;
            Supplier<Long>  delayGetter = () -> 1000L;
            TapStreamReadConsumer<List<TapEvent>, Object> consumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayGetter, isAlive, consumer, obsLogger);
            Assertions.assertEquals(1000L, acceptor.getDelayMs());
        }
    }

    @Nested
    class AcceptSingleEventTest {
        @Test
        void testAcceptNullEvent() {
            BatchAcceptor.ValueGetter<Integer> batchSizeGetter = () -> 1;
            Supplier<Long>  delayGetter = () -> 1000L;
            TapStreamReadConsumer<List<TapEvent>, Object> consumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayGetter, isAlive, consumer, obsLogger);
            acceptor.accept((TapEvent) null, new Object());

            verify(consumer, never()).accept(anyList(), any());
        }

        @Test
        void testAcceptSingleEventTriggerBySize() {
            BatchAcceptor.ValueGetter<Integer> batchSizeGetter = () -> 1;
            Supplier<Long>  delayGetter = () -> 10_000L;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> consumer = mock(TapStreamReadConsumer.class);
            ThreadPoolExecutorEx sourceRunner = mock(ThreadPoolExecutorEx.class);
            when(sourceRunner.submit(any(Runnable.class))).thenReturn(mock(Future.class));
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayGetter, isAlive, consumer, obsLogger);
            acceptor.startMonitor(sourceRunner);
            TapEvent event = mock(TapEvent.class);
            Object offset = new Object();
            acceptor.accept(event, offset);

            verify(consumer, times(0)).accept(anyList(), eq(offset));
        }
    }

    @Nested
    class AcceptCollectionEventTest {
        @Test
        void testAcceptEmptyOrAllNullCollection() {
            BatchAcceptor.ValueGetter<Integer> batchSizeGetter = () -> 2;
            Supplier<Long>  delayGetter = () -> 1000L;
            TapStreamReadConsumer<List<TapEvent>, Object> consumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayGetter, isAlive, consumer, obsLogger);
            acceptor.accept(new ArrayList<>(), new Object());

            List<TapEvent> listWithNull = new ArrayList<>();
            listWithNull.add(null);
            acceptor.accept(listWithNull, new Object());

            verify(consumer, never()).accept(anyList(), any());
        }

        @Test
        void testAcceptCollectionFilterNullAndTriggerBySize() {
            BatchAcceptor.ValueGetter<Integer> batchSizeGetter = () -> 2;
            Supplier<Long>  delayGetter = () -> 10_000L;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> consumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayGetter, isAlive, consumer, obsLogger);

            List<TapEvent> events = new ArrayList<>();
            events.add(mock(TapEvent.class));
            events.add(null);
            events.add(mock(TapEvent.class));

            Object offset = new Object();
            acceptor.accept(events, offset);

            verify(consumer, times(0)).accept(anyList(), eq(offset));
        }

        @Test
        void testAcceptCollectionTriggerByDelay() throws InterruptedException {
            BatchAcceptor.ValueGetter<Integer> batchSizeGetter = () -> 1000;
            Supplier<Long>  delayGetter = () -> 10L;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> consumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayGetter, isAlive, consumer, obsLogger);
            Object offset = new Object();

            List<TapEvent> events = new ArrayList<>();
            events.add(mock(TapEvent.class));
            acceptor.accept(events, offset);

            Thread.sleep(20L);

            List<TapEvent> moreEvents = new ArrayList<>();
            moreEvents.add(mock(TapEvent.class));
            acceptor.accept(moreEvents, offset);

            verify(consumer, times(0)).accept(anyList(), eq(offset));
        }

        @Test
        void testAcceptNullList() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            acceptor.active = true;
            acceptor.accept((List<TapEvent>) null, new Object());
            verify(consumer, never()).accept(anyList(), any());
        }

        @Test
        void testAcceptWhenNotActive() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            acceptor.active = false;
            List<TapEvent> events = new ArrayList<>();
            events.add(mock(TapEvent.class));
            acceptor.accept(events, new Object());
            verify(consumer, never()).accept(anyList(), any());
        }
    }

    @Nested
    class ConsumerTest {
        @Test
        void testConsumerExitsWhenNotAlive() {
            AtomicInteger callCount = new AtomicInteger(0);
            Predicate<Boolean> notAlive = b -> {
                callCount.incrementAndGet();
                return false;
            };
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, notAlive, consumer, obsLogger);
            acceptor.active = true;
            acceptor.consumer();
            Assertions.assertFalse(acceptor.active);
            Assertions.assertEquals(1, callCount.get());
        }

        @Test
        void testConsumerProcessesEventsAndTriggersByBatchSize() {
            AtomicInteger aliveCallCount = new AtomicInteger(0);
            Predicate<Boolean> controlledAlive = b -> {
                int count = aliveCallCount.incrementAndGet();
                return count <= 5;
            };
            BatchAcceptor.ValueGetter<Integer> batchSize = () -> 2;
            Supplier<Long>  delay = () -> 10000L;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> mockConsumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSize, delay, controlledAlive, mockConsumer, obsLogger);
            acceptor.active = true;

            TapEvent event1 = mock(TapEvent.class);
            TapEvent event2 = mock(TapEvent.class);
            Object offset = "test-offset";

            @SuppressWarnings("unchecked")
            LinkedBlockingQueue<BatchAcceptor.EventInfo> pipeline =
                    (LinkedBlockingQueue<BatchAcceptor.EventInfo>) ReflectionTestUtils.getField(acceptor, "pipeline");
            pipeline.add(new BatchAcceptor.EventInfo(event1, offset));
            pipeline.add(new BatchAcceptor.EventInfo(event2, offset));

            acceptor.consumer();

            verify(mockConsumer, atLeastOnce()).accept(anyList(), eq(offset));
        }

        @Test
        void testConsumerProcessesEventsAndTriggersByDelay() throws InterruptedException {
            AtomicInteger aliveCallCount = new AtomicInteger(0);
            Predicate<Boolean> controlledAlive = b -> {
                int count = aliveCallCount.incrementAndGet();
                return count <= 3;
            };
            BatchAcceptor.ValueGetter<Integer> batchSize = () -> 1000;
            Supplier<Long>  delay = () -> 1L;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> mockConsumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSize, delay, controlledAlive, mockConsumer, obsLogger);
            acceptor.active = true;

            TapEvent event = mock(TapEvent.class);
            Object offset = "test-offset";

            @SuppressWarnings("unchecked")
            LinkedBlockingQueue<BatchAcceptor.EventInfo> pipeline =
                    (LinkedBlockingQueue<BatchAcceptor.EventInfo>) ReflectionTestUtils.getField(acceptor, "pipeline");
            pipeline.add(new BatchAcceptor.EventInfo(event, offset));

            Thread.sleep(10);
            acceptor.consumer();

            Assertions.assertFalse(acceptor.active);
        }

        @Test
        void testConsumerWithNullOffset() {
            AtomicInteger aliveCallCount = new AtomicInteger(0);
            Predicate<Boolean> controlledAlive = b -> {
                int count = aliveCallCount.incrementAndGet();
                return count <= 3;
            };
            BatchAcceptor.ValueGetter<Integer> batchSize = () -> 1;
            Supplier<Long>  delay = () -> 1000L;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> mockConsumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSize, delay, controlledAlive, mockConsumer, obsLogger);
            acceptor.active = true;

            TapEvent event = mock(TapEvent.class);

            @SuppressWarnings("unchecked")
            LinkedBlockingQueue<BatchAcceptor.EventInfo> pipeline =
                    (LinkedBlockingQueue<BatchAcceptor.EventInfo>) ReflectionTestUtils.getField(acceptor, "pipeline");
            pipeline.add(new BatchAcceptor.EventInfo(event, null));

            acceptor.consumer();

            verify(mockConsumer, atLeastOnce()).accept(anyList(), isNull());
        }

        @Test
        void testConsumerWithEmptyPipeline() {
            AtomicInteger aliveCallCount = new AtomicInteger(0);
            Predicate<Boolean> controlledAlive = b -> {
                int count = aliveCallCount.incrementAndGet();
                return count <= 2;
            };
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, controlledAlive, consumer, obsLogger);
            acceptor.active = true;

            acceptor.consumer();

            Assertions.assertFalse(acceptor.active);
            verify(consumer, never()).accept(anyList(), any());
        }

        @Test
        void testConsumerWithCollectionEvents() {
            AtomicInteger aliveCallCount = new AtomicInteger(0);
            Predicate<Boolean> controlledAlive = b -> {
                int count = aliveCallCount.incrementAndGet();
                return count <= 3;
            };
            BatchAcceptor.ValueGetter<Integer> batchSize = () -> 2;
            Supplier<Long>  delay = () -> 10000L;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> mockConsumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSize, delay, controlledAlive, mockConsumer, obsLogger);
            acceptor.active = true;

            List<TapEvent> eventList = new ArrayList<>();
            eventList.add(mock(TapEvent.class));
            eventList.add(mock(TapEvent.class));
            Object offset = "test-offset";

            @SuppressWarnings("unchecked")
            LinkedBlockingQueue<BatchAcceptor.EventInfo> pipeline =
                    (LinkedBlockingQueue<BatchAcceptor.EventInfo>) ReflectionTestUtils.getField(acceptor, "pipeline");
            pipeline.add(new BatchAcceptor.EventInfo(eventList, offset));

            acceptor.consumer();

            verify(mockConsumer, atLeastOnce()).accept(anyList(), eq(offset));
        }

        @Test
        void testConsumerWithNullEventInfo() {
            AtomicInteger aliveCallCount = new AtomicInteger(0);
            Predicate<Boolean> controlledAlive = b -> {
                int count = aliveCallCount.incrementAndGet();
                return count <= 3;
            };
            BatchAcceptor.ValueGetter<Integer> batchSize = () -> 10;
            Supplier<Long>  delay = () -> 1L;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> mockConsumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = spy(new BatchAcceptor(batchSize, delay, controlledAlive, mockConsumer, obsLogger));
            acceptor.active = true;

            @SuppressWarnings("unchecked")
            LinkedBlockingQueue<BatchAcceptor.EventInfo> pipeline =
                    (LinkedBlockingQueue<BatchAcceptor.EventInfo>) ReflectionTestUtils.getField(acceptor, "pipeline");
            pipeline.add(new BatchAcceptor.EventInfo(mock(TapEvent.class), "offset"));

            doReturn(null).when(acceptor).pollSingle(anyLong());

            acceptor.consumer();

            Assertions.assertFalse(acceptor.active);
        }

        @Test
        void testConsumerEventsNotEmptyButNotTimeout() {
            AtomicInteger aliveCallCount = new AtomicInteger(0);
            Predicate<Boolean> controlledAlive = b -> {
                int count = aliveCallCount.incrementAndGet();
                return count <= 2;
            };
            BatchAcceptor.ValueGetter<Integer> batchSize = () -> 100;
            Supplier<Long>  delay = () -> 100000L;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> mockConsumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSize, delay, controlledAlive, mockConsumer, obsLogger);
            acceptor.active = true;

            TapEvent event = mock(TapEvent.class);
            Object offset = "test-offset";

            @SuppressWarnings("unchecked")
            LinkedBlockingQueue<BatchAcceptor.EventInfo> pipeline =
                    (LinkedBlockingQueue<BatchAcceptor.EventInfo>) ReflectionTestUtils.getField(acceptor, "pipeline");
            pipeline.add(new BatchAcceptor.EventInfo(event, offset));

            acceptor.consumer();

            Assertions.assertFalse(acceptor.active);
            verify(mockConsumer, never()).accept(anyList(), any());
        }

        @Test
        void testConsumerEventsNotEmptyTimeout() {
            AtomicInteger aliveCallCount = new AtomicInteger(0);
            Predicate<Boolean> controlledAlive = b -> {
                int count = aliveCallCount.incrementAndGet();
                return count <= 2;
            };
            BatchAcceptor.ValueGetter<Integer> batchSize = () -> 100000000;
            Supplier<Long>  delay = () -> 0L;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> mockConsumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSize, delay, controlledAlive, mockConsumer, obsLogger);
            acceptor.active = true;

            TapEvent event = mock(TapEvent.class);
            Object offset = "test-offset";

            @SuppressWarnings("unchecked")
            LinkedBlockingQueue<BatchAcceptor.EventInfo> pipeline =
                    (LinkedBlockingQueue<BatchAcceptor.EventInfo>) ReflectionTestUtils.getField(acceptor, "pipeline");
            pipeline.add(new BatchAcceptor.EventInfo(event, offset));

            acceptor.consumer();

            Assertions.assertFalse(acceptor.active);
        }

        @Test
        void testConsumerCountNotReachBatchSize() {
            AtomicInteger aliveCallCount = new AtomicInteger(0);
            Predicate<Boolean> controlledAlive = b -> {
                int count = aliveCallCount.incrementAndGet();
                return count <= 4;
            };
            BatchAcceptor.ValueGetter<Integer> batchSize = () -> 10;
            Supplier<Long>  delay = () -> 100000L;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> mockConsumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSize, delay, controlledAlive, mockConsumer, obsLogger);
            acceptor.active = true;

            TapEvent event = mock(TapEvent.class);
            Object offset = "test-offset";

            @SuppressWarnings("unchecked")
            LinkedBlockingQueue<BatchAcceptor.EventInfo> pipeline =
                    (LinkedBlockingQueue<BatchAcceptor.EventInfo>) ReflectionTestUtils.getField(acceptor, "pipeline");
            pipeline.add(new BatchAcceptor.EventInfo(event, offset));

            acceptor.consumer();

            Assertions.assertFalse(acceptor.active);
            verify(mockConsumer, never()).accept(anyList(), any());
        }
    }

    @Nested
    class FixDelayTest {
        @Test
        void testFixDelayWithZero() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            long result = acceptor.fixDelay(0L);
            Assertions.assertEquals(1000L, result);
        }

        @Test
        void testFixDelayWithNegative() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            long result = acceptor.fixDelay(-100L);
            Assertions.assertEquals(1000L, result);
        }

        @Test
        void testFixDelayWithPositive() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            long result = acceptor.fixDelay(500L);
            Assertions.assertEquals(500L, result);
        }
    }

    @Nested
    class ClassificationTest {
        @Test
        void testClassificationWithSingleEvent() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            TapEvent event = mock(TapEvent.class);
            BatchAcceptor.EventInfo eventInfo = new BatchAcceptor.EventInfo(event, "offset");
            List<TapEvent> events = new ArrayList<>();

            acceptor.classification(eventInfo, events);

            Assertions.assertEquals(1, events.size());
            Assertions.assertSame(event, events.get(0));
        }

        @Test
        void testClassificationWithCollection() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            List<TapEvent> eventList = new ArrayList<>();
            eventList.add(mock(TapEvent.class));
            eventList.add(mock(TapEvent.class));
            BatchAcceptor.EventInfo eventInfo = new BatchAcceptor.EventInfo(eventList, "offset");
            List<TapEvent> events = new ArrayList<>();

            acceptor.classification(eventInfo, events);

            Assertions.assertEquals(2, events.size());
        }
    }

    @Nested
    class OfferSingleTest {
        @Test
        void testOfferSingleSuccess() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            TapEvent event = mock(TapEvent.class);
            BatchAcceptor.EventInfo eventInfo = new BatchAcceptor.EventInfo(event, "offset");

            Assertions.assertDoesNotThrow(() -> acceptor.offerSingle(eventInfo));
        }

        @Test
        void testOfferSingleInterrupted() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            TapEvent event = mock(TapEvent.class);
            BatchAcceptor.EventInfo eventInfo = new BatchAcceptor.EventInfo(event, "offset");

            Thread.currentThread().interrupt();
            Assertions.assertDoesNotThrow(() -> acceptor.offerSingle(eventInfo));
            Assertions.assertTrue(Thread.interrupted());
        }

        @Test
        void testOfferSingleFailed() {
            BatchAcceptor acceptor = spy(new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger));
            @SuppressWarnings("unchecked")
            LinkedBlockingQueue<BatchAcceptor.EventInfo> fullPipeline = mock(LinkedBlockingQueue.class);
            try {
                when(fullPipeline.offer(any(), anyLong(), any())).thenReturn(false);
            } catch (InterruptedException e) {
                // ignore
            }
            ReflectionTestUtils.setField(acceptor, "pipeline", fullPipeline);

            TapEvent event = mock(TapEvent.class);
            BatchAcceptor.EventInfo eventInfo = new BatchAcceptor.EventInfo(event, "offset");

            Assertions.assertDoesNotThrow(() -> acceptor.offerSingle(eventInfo));
            verify(obsLogger, times(1)).debug(anyString());
        }
    }

    @Nested
    class PollSingleTest {
        @Test
        void testPollSingleFromEmptyQueue() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            BatchAcceptor.EventInfo result = acceptor.pollSingle(1);
            Assertions.assertNull(result);
        }

        @Test
        void testPollSingleFromNonEmptyQueue() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            TapEvent event = mock(TapEvent.class);
            BatchAcceptor.EventInfo eventInfo = new BatchAcceptor.EventInfo(event, "offset");

            @SuppressWarnings("unchecked")
            LinkedBlockingQueue<BatchAcceptor.EventInfo> pipeline =
                    (LinkedBlockingQueue<BatchAcceptor.EventInfo>) ReflectionTestUtils.getField(acceptor, "pipeline");
            pipeline.add(eventInfo);

            BatchAcceptor.EventInfo result = acceptor.pollSingle(1);
            Assertions.assertNotNull(result);
            Assertions.assertSame(event, result.getEvents());
        }

        @Test
        void testPollSingleInterrupted() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);

            Thread.currentThread().interrupt();
            BatchAcceptor.EventInfo result = acceptor.pollSingle(1);
            Assertions.assertNull(result);
            Assertions.assertTrue(Thread.interrupted());
        }
    }

    @Nested
    class StartMonitorTest {
        @Test
        void testStartMonitor() {
            ThreadPoolExecutorEx sourceRunner = mock(ThreadPoolExecutorEx.class);
            Future mockFuture = mock(Future.class);
            when(sourceRunner.submit(any(Runnable.class))).thenReturn(mockFuture);

            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            acceptor.startMonitor(sourceRunner);

            Assertions.assertTrue(acceptor.active);
            Assertions.assertSame(mockFuture, acceptor.monitor);
            verify(sourceRunner, times(1)).submit(any(Runnable.class));
        }
    }

    @Nested
    class CloseTest {
        @Test
        void testClose() {
            Future<?> mockFuture = mock(Future.class);
            when(mockFuture.cancel(true)).thenReturn(true);

            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            acceptor.active = true;
            acceptor.monitor = mockFuture;

            acceptor.close();

            Assertions.assertFalse(acceptor.active);
            verify(mockFuture, times(1)).cancel(true);
        }
        @Test
        void testCloseNull() {
            Future<?> mockFuture = mock(Future.class);
            when(mockFuture.cancel(true)).thenReturn(true);

            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            acceptor.active = true;

            acceptor.close();

            Assertions.assertFalse(acceptor.active);
            verify(mockFuture, times(0)).cancel(true);
        }
    }

    @Nested
    class EventInfoTest {
        @Test
        void testEventInfoWithSingleEvent() {
            TapEvent event = mock(TapEvent.class);
            Object offset = "test-offset";
            BatchAcceptor.EventInfo eventInfo = new BatchAcceptor.EventInfo(event, offset);

            Assertions.assertSame(event, eventInfo.getEvents());
            Assertions.assertEquals(offset, eventInfo.getOffset());
            Assertions.assertEquals(1, eventInfo.getSize());
        }

        @Test
        void testEventInfoWithList() {
            List<TapEvent> events = new ArrayList<>();
            events.add(mock(TapEvent.class));
            events.add(mock(TapEvent.class));
            events.add(mock(TapEvent.class));
            Object offset = "test-offset";
            BatchAcceptor.EventInfo eventInfo = new BatchAcceptor.EventInfo(events, offset);

            Assertions.assertSame(events, eventInfo.getEvents());
            Assertions.assertEquals(offset, eventInfo.getOffset());
            Assertions.assertEquals(3, eventInfo.getSize());
        }
    }

    @Nested
    class AcceptSingleEventWhenActiveTest {
        @Test
        void testAcceptSingleEventWhenNotActive() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            acceptor.active = false;
            TapEvent event = mock(TapEvent.class);
            acceptor.accept(event, new Object());
            verify(consumer, never()).accept(anyList(), any());
        }

        @Test
        void testAcceptSingleEventWhenActive() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            acceptor.active = true;
            TapEvent event = mock(TapEvent.class);
            Object offset = "test-offset";

            acceptor.accept(event, offset);

            @SuppressWarnings("unchecked")
            LinkedBlockingQueue<BatchAcceptor.EventInfo> pipeline =
                    (LinkedBlockingQueue<BatchAcceptor.EventInfo>) ReflectionTestUtils.getField(acceptor, "pipeline");
            Assertions.assertEquals(1, pipeline.size());
        }
    }

    @Nested
    class AcceptListEventWhenActiveTest {
        @Test
        void testAcceptListEventWhenActive() {
            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayMsGetter, isAlive, consumer, obsLogger);
            acceptor.active = true;
            List<TapEvent> events = new ArrayList<>();
            events.add(mock(TapEvent.class));
            events.add(mock(TapEvent.class));
            Object offset = "test-offset";

            acceptor.accept(events, offset);

            @SuppressWarnings("unchecked")
            LinkedBlockingQueue<BatchAcceptor.EventInfo> pipeline =
                    (LinkedBlockingQueue<BatchAcceptor.EventInfo>) ReflectionTestUtils.getField(acceptor, "pipeline");
            Assertions.assertEquals(1, pipeline.size());
        }
    }

    @Nested
    class ConstructorTest {
        @Test
        void testConstructor() {
            BatchAcceptor.ValueGetter<Integer> batchSize = () -> 100;
            Supplier<Long>  delay = () -> 500L;
            Predicate<Boolean> alive = b -> true;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> mockConsumer = mock(TapStreamReadConsumer.class);
            ObsLogger logger = mock(ObsLogger.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSize, delay, alive, mockConsumer, logger);

            Assertions.assertNotNull(acceptor);
            Assertions.assertFalse(acceptor.active);
            Assertions.assertNull(acceptor.monitor);
            Assertions.assertEquals(500L, acceptor.getDelayMs());
        }
    }
}

