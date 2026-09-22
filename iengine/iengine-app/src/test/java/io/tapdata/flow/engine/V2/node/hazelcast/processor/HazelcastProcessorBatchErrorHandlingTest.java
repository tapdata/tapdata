package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.processor.error.ScriptProcessorExCode_30;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.UnionProcessorNode;
import io.tapdata.aspect.SkipErrorProcessAspect;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.exception.TapCodeException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class HazelcastProcessorBatchErrorHandlingTest {

    @Test
    void recordLevelFailureInBatchShouldBeInterceptedAndAllowFollowingEvent() {
        TapdataEvent failedEvent = event();
        TapdataEvent followingEvent = event();
        CapturingProcessorNode processorNode = new CapturingProcessorNode(failedEvent,
                new TapCodeException(ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESS_FAILED));

        List<TapdataEvent> results = assertDoesNotThrow(() -> processorNode.batchProcess(
                List.of(new HazelcastProcessorBaseNode.BatchEventWrapper(failedEvent),
                        new HazelcastProcessorBaseNode.BatchEventWrapper(followingEvent))));

        assertEquals(List.of(failedEvent), processorNode.interceptedEvents);
        assertEquals(List.of(followingEvent), processorNode.processedEvents);
        assertEquals(List.of(followingEvent), results);
    }

    @Test
    void rawThrowableInBatchShouldBeInterceptedAndAllowFollowingEvent() {
        TapdataEvent failedEvent = event();
        TapdataEvent followingEvent = event();
        CapturingProcessorNode processorNode = new CapturingProcessorNode(failedEvent,
                new AssertionError("processor runtime failure"));

        List<TapdataEvent> results = assertDoesNotThrow(() -> processorNode.batchProcess(
                List.of(new HazelcastProcessorBaseNode.BatchEventWrapper(failedEvent),
                        new HazelcastProcessorBaseNode.BatchEventWrapper(followingEvent))));

        assertEquals(List.of(failedEvent), processorNode.interceptedEvents);
        assertEquals(List.of(followingEvent), processorNode.processedEvents);
        assertEquals(List.of(followingEvent), results);
    }

    private static TapdataEvent event() {
        TapdataEvent event = new TapdataEvent();
        event.setTapEvent(new TapInsertRecordEvent());
        return event;
    }

    private static final class CapturingProcessorNode extends HazelcastProcessorBaseNode {
        private final TapdataEvent failingEvent;
        private final List<TapdataEvent> interceptedEvents = new ArrayList<>();
        private final List<TapdataEvent> processedEvents = new ArrayList<>();
        private final Node<?> node = new UnionProcessorNode();
        private final Throwable failure;

        private CapturingProcessorNode(TapdataEvent failingEvent, Throwable failure) {
            super(null);
            this.failingEvent = failingEvent;
            this.failure = failure;
        }

        @Override
        protected void tryProcess(TapdataEvent tapdataEvent,
            BiConsumer<TapdataEvent, ProcessResult> consumer) {
            if (tapdataEvent == failingEvent) {
                if (failure instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                if (failure instanceof Error error) {
                    throw error;
                }
                throw new RuntimeException(failure);
            }
            processedEvents.add(tapdataEvent);
            consumer.accept(tapdataEvent, ProcessResult.create());
        }

        @Override
        public Node<?> getNode() {
            return node;
        }

        @Override
        public boolean needTransformValue() {
            return false;
        }

        @Override
        public AspectInterceptResult executeAspect(Aspect aspect) {
            if (aspect instanceof SkipErrorProcessAspect processAspect) {
                interceptedEvents.add(processAspect.getInputEvent());
                return new AspectInterceptResult().intercepted(true);
            }
            return null;
        }
    }
}
