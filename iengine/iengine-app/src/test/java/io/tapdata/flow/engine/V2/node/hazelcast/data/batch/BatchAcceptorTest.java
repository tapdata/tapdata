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

