package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.aspect.ProcessorNodeProcessAspect;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.exception.TapCodeException;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.mockito.Mockito.*;

public class HazelcastUnwindProcessNodeTest extends BaseHazelcastNodeTest {
    private HazelcastUnwindProcessNode hazelcastUnwindProcessNode;
    @BeforeEach
    void beforeSetUp() {
        super.allSetup();
        hazelcastUnwindProcessNode = new HazelcastUnwindProcessNode(processorBaseContext);
    }

    @Nested
    @DisplayName("Method tryProcess test")
    class tryProcessByBatchEventWrapperTest {
        @Test
        void test_main() throws CloneNotSupportedException {
            List<HazelcastProcessorBaseNode.BatchEventWrapper> tapdataEvents = new ArrayList<>();
            TapdataEvent tapdataEvent = new TapdataEvent();
            tapdataEvent.setTapEvent(new TapInsertRecordEvent());
            ProcessorNodeProcessAspect processAspect = new ProcessorNodeProcessAspect();
            HazelcastProcessorBaseNode.BatchEventWrapper batchEventWrapper = spy(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent,processAspect));
            tapdataEvents.add(batchEventWrapper);
            Consumer<List<HazelcastProcessorBaseNode.BatchProcessResult>> consumer = (batchProcessResults) -> {
                Assertions.assertEquals(1,batchProcessResults.size());
            };
            hazelcastUnwindProcessNode.tryProcess(tapdataEvents,consumer);
            verify(batchEventWrapper,times(1)).clone();
        }

        @Test
        void test_cloneError() throws CloneNotSupportedException {
            List<HazelcastProcessorBaseNode.BatchEventWrapper> tapdataEvents = new ArrayList<>();
            TapdataEvent tapdataEvent = new TapdataEvent();
            tapdataEvent.setTapEvent(new TapInsertRecordEvent());
            ProcessorNodeProcessAspect processAspect = new ProcessorNodeProcessAspect();
            HazelcastProcessorBaseNode.BatchEventWrapper batchEventWrapper = spy(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent,processAspect));
            when(batchEventWrapper.clone()).thenThrow(new RuntimeException("clone error"));
            tapdataEvents.add(batchEventWrapper);
            Consumer<List<HazelcastProcessorBaseNode.BatchProcessResult>> consumer = (batchProcessResults) -> {
            };
            Assertions.assertThrows(TapCodeException.class,()->hazelcastUnwindProcessNode.tryProcess(tapdataEvents,consumer));
        }
    }

    @Test
    void testNeedCopyBatchEventWrapper(){
        Assertions.assertTrue(hazelcastUnwindProcessNode.needCopyBatchEventWrapper());
    }
}
