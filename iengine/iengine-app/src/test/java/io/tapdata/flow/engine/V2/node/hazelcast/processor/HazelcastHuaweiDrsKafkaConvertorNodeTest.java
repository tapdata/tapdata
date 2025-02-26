package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.huawei.drs.kafka.FromDBType;
import com.tapdata.huawei.drs.kafka.StoreType;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.HuaweiDrsKafkaConvertorNode;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.huawei.drs.kafka.ISerialization;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.function.BiConsumer;

import static org.mockito.Mockito.spy;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/25 17:52 Create
 */
class HazelcastHuaweiDrsKafkaConvertorNodeTest {
    ProcessorBaseContext processorBaseContext;
    HuaweiDrsKafkaConvertorNode node;
    HazelcastHuaweiDrsKafkaConvertorNode processor;
    HazelcastHuaweiDrsKafkaConvertorNode mockProcessor;

    @BeforeEach
    void setUp() {
        node = new HuaweiDrsKafkaConvertorNode();
        node.setStoreType(StoreType.JSON.name());
        node.setFromDBType(FromDBType.MYSQL.name());
        processorBaseContext = Mockito.mock(ProcessorBaseContext.class);
        Mockito.when(processorBaseContext.getNode()).thenReturn((Node) node);
        processor = new HazelcastHuaweiDrsKafkaConvertorNode(processorBaseContext);
        mockProcessor = spy(processor);
    }

    @Test
    void testNotRecordEvent() {
        TapdataEvent tapdataEvent = new TapdataEvent();
        BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = Mockito.mock(BiConsumer.class);

        Mockito.doReturn(null).when(mockProcessor).getProcessResult(Mockito.any());
        mockProcessor.tryProcess(tapdataEvent, consumer);
        Mockito.verify(consumer, Mockito.times(1)).accept(Mockito.any(), Mockito.any());
    }

    @Test
    void testSuccess() {
        TapdataEvent tapdataEvent = new TapdataEvent();
        tapdataEvent.setTapEvent(TapInsertRecordEvent.create());
        BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = Mockito.mock(BiConsumer.class);
        ISerialization serialization = Mockito.mock(ISerialization.class);

        Mockito.doReturn(null).when(mockProcessor).getProcessResult(Mockito.any());
        Mockito.doReturn(serialization).when(mockProcessor).getSerialization();
        mockProcessor.tryProcess(tapdataEvent, consumer);
        Mockito.verify(serialization, Mockito.times(1)).process(Mockito.eq(tapdataEvent), Mockito.eq(consumer), Mockito.any());
    }
}
