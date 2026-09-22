package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataHeartbeatEvent;
import io.tapdata.exception.TapCodeException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HazelcastMergeNodeDqlCaptureTest {

    @Test
    void failedLookupShouldBeIsolatedAndFollowingEventShouldContinue() {
        TapdataEvent failedEvent = heartbeat();
        TapdataEvent followingEvent = heartbeat();
        CapturingMergeNode mergeNode = new CapturingMergeNode(failedEvent);
        List<HazelcastProcessorBaseNode.BatchEventWrapper> lookupEvents = List.of(
                new HazelcastProcessorBaseNode.BatchEventWrapper(failedEvent));
        List<CompletableFuture<Void>> lookupFutures = new ArrayList<>();

        mergeNode.doBatchLookUpConcurrent(lookupEvents, lookupFutures);

        List<HazelcastProcessorBaseNode.BatchProcessResult> batchResults = List.of(
                new HazelcastProcessorBaseNode.BatchProcessResult(
                        new HazelcastProcessorBaseNode.BatchEventWrapper(failedEvent), null),
                new HazelcastProcessorBaseNode.BatchProcessResult(
                        new HazelcastProcessorBaseNode.BatchEventWrapper(followingEvent), null));
        List<HazelcastProcessorBaseNode.BatchProcessResult> acceptedResults = new ArrayList<>();
        Consumer<List<HazelcastProcessorBaseNode.BatchProcessResult>> consumer = acceptedResults::addAll;

        mergeNode.acceptIfNeed(consumer, batchResults, lookupFutures);

        assertEquals(List.of(failedEvent), mergeNode.interceptedEvents);
        assertTrue(mergeNode.handledErrors.isEmpty());
        assertEquals(List.of(followingEvent), acceptedResults.stream()
                .map(result -> result.getBatchEventWrapper().getTapdataEvent())
                .toList());
    }

    private static TapdataEvent heartbeat() {
        return new TapdataHeartbeatEvent();
    }

    private static final class CapturingMergeNode extends HazelcastMergeNode {
        private final TapdataEvent failedEvent;
        private final List<TapdataEvent> interceptedEvents = new ArrayList<>();
        private final List<Throwable> handledErrors = new ArrayList<>();

        private CapturingMergeNode(TapdataEvent failedEvent) {
            super(null);
            this.failedEvent = failedEvent;
        }

        @Override
        protected boolean needLookup(TapdataEvent tapdataEvent) {
            return tapdataEvent == failedEvent;
        }

        @Override
        protected CompletableFuture<Void> lookupAndWrapMergeInfoConcurrent(TapdataEvent tapdataEvent) {
            return CompletableFuture.failedFuture(new ExecutionException(
                    new IllegalStateException("lookup failed")));
        }

        @Override
        protected boolean interceptProcessorError(TapdataEvent tapdataEvent, Throwable error) {
            interceptedEvents.add(tapdataEvent);
            return true;
        }

        @Override
        public synchronized TapCodeException errorHandle(Throwable throwable) {
            handledErrors.add(throwable);
            return null;
        }

    }
}
