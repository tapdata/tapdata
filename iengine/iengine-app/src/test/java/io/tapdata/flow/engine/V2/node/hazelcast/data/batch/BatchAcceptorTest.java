package io.tapdata.flow.engine.V2.node.hazelcast.data.batch;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.consumer.TapStreamReadConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

class BatchAcceptorTest {

    @Nested
    class GetDelayMsTest {
        @Test
        void testGetDelayMs() {
            BatchAcceptor.ValueGetter<Integer> batchSizeGetter = () -> 10;
            BatchAcceptor.ValueGetter<Integer> delayGetter = () -> 1000;
            TapStreamReadConsumer<List<TapEvent>, Object> consumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayGetter, consumer);
            Assertions.assertEquals(1000L, acceptor.getDelayMs());
        }
    }

    @Nested
    class AcceptSingleEventTest {
        @Test
        void testAcceptNullEvent() {
            BatchAcceptor.ValueGetter<Integer> batchSizeGetter = () -> 1;
            BatchAcceptor.ValueGetter<Integer> delayGetter = () -> 1000;
            TapStreamReadConsumer<List<TapEvent>, Object> consumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayGetter, consumer);
            acceptor.accept((TapEvent) null, new Object());

            verify(consumer, never()).accept(anyList(), any());
        }

        @Test
        void testAcceptSingleEventTriggerBySize() {
            BatchAcceptor.ValueGetter<Integer> batchSizeGetter = () -> 1;
            BatchAcceptor.ValueGetter<Integer> delayGetter = () -> 10_000;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> consumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayGetter, consumer);
            TapEvent event = mock(TapEvent.class);
            Object offset = new Object();
            acceptor.accept(event, offset);

            verify(consumer, times(1)).accept(anyList(), eq(offset));
        }
    }

    @Nested
    class AcceptCollectionEventTest {
        @Test
        void testAcceptEmptyOrAllNullCollection() {
            BatchAcceptor.ValueGetter<Integer> batchSizeGetter = () -> 2;
            BatchAcceptor.ValueGetter<Integer> delayGetter = () -> 1000;
            TapStreamReadConsumer<List<TapEvent>, Object> consumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayGetter, consumer);
            acceptor.accept(new ArrayList<>(), new Object());

            List<TapEvent> listWithNull = new ArrayList<>();
            listWithNull.add(null);
            acceptor.accept(listWithNull, new Object());

            verify(consumer, never()).accept(anyList(), any());
        }

        @Test
        void testAcceptCollectionFilterNullAndTriggerBySize() {
            BatchAcceptor.ValueGetter<Integer> batchSizeGetter = () -> 2;
            BatchAcceptor.ValueGetter<Integer> delayGetter = () -> 10_000;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> consumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayGetter, consumer);

            List<TapEvent> events = new ArrayList<>();
            events.add(mock(TapEvent.class));
            events.add(null);
            events.add(mock(TapEvent.class));

            Object offset = new Object();
            acceptor.accept(events, offset);

            verify(consumer, times(1)).accept(anyList(), eq(offset));
        }

        @Test
        void testAcceptCollectionTriggerByDelay() throws InterruptedException {
            BatchAcceptor.ValueGetter<Integer> batchSizeGetter = () -> 1000;
            BatchAcceptor.ValueGetter<Integer> delayGetter = () -> 10;
            @SuppressWarnings("unchecked")
            TapStreamReadConsumer<List<TapEvent>, Object> consumer = mock(TapStreamReadConsumer.class);

            BatchAcceptor acceptor = new BatchAcceptor(batchSizeGetter, delayGetter, consumer);
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
    }
}

