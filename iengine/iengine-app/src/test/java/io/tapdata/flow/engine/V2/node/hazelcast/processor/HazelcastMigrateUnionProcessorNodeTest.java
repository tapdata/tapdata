package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.MigrateUnionProcessorNode;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.function.BiConsumer;

import static org.mockito.Mockito.*;

public class HazelcastMigrateUnionProcessorNodeTest extends BaseHazelcastNodeTest {
    HazelcastMigrateUnionProcessorNode hazelcastMigrateUnionProcessorNode;
    MigrateUnionProcessorNode unionProcessorNode;

    @BeforeEach
    void beforeEach() {
        super.allSetup();
        unionProcessorNode = new MigrateUnionProcessorNode();
        unionProcessorNode.setTableName("union_test");
        when(dataProcessorContext.getNode()).thenReturn((Node) unionProcessorNode);
        CommonUtils.setProperty("app_type", "DAAS");
        hazelcastMigrateUnionProcessorNode = new HazelcastMigrateUnionProcessorNode(dataProcessorContext);
        ReflectionTestUtils.setField(hazelcastMigrateUnionProcessorNode, "obsLogger", mockObsLogger);
        ReflectionTestUtils.setField(hazelcastMigrateUnionProcessorNode, "clientMongoOperator", mockClientMongoOperator);
    }
    @Test
    void test_main(){
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapInsertRecordEvent insertRecordEvent = new TapInsertRecordEvent();
        insertRecordEvent.setTableId("test");
        tapdataEvent.setTapEvent(insertRecordEvent);
        BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = (tapdataEvent1, processResult)->{
            TapBaseEvent tapBaseEvent = (TapBaseEvent) tapdataEvent1.getTapEvent();
            Assertions.assertEquals("union_test",tapBaseEvent.getTableId());
        };
        hazelcastMigrateUnionProcessorNode.tryProcess(tapdataEvent,consumer);
    }

    @Test
    void test_DDLEvent(){
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapCreateTableEvent createTableEvent = new TapCreateTableEvent();
        tapdataEvent.setTapEvent(createTableEvent);
        BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = mock(BiConsumer.class);
        hazelcastMigrateUnionProcessorNode.tryProcess(tapdataEvent,consumer);
        verify(consumer,times(0)).accept(any(),any());
    }
}
