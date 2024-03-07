package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.process.AddDateFieldProcessorNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class HazelcastAddDateFieldProcessNodeTest extends BaseHazelcastNodeTest {
    private HazelcastAddDateFieldProcessNode addDateFieldProcessNode;
    @BeforeEach
    void beforeSetUp(){
        super.allSetup();
        AddDateFieldProcessorNode addDateFieldProcessorNode = new AddDateFieldProcessorNode();
        addDateFieldProcessorNode.setDateFieldName("dateTime");
        ReflectionTestUtils.setField(processorBaseContext,"node",addDateFieldProcessorNode);
        doCallRealMethod().when(processorBaseContext).getNode();
        addDateFieldProcessNode=new HazelcastAddDateFieldProcessNode(processorBaseContext);
    }
    @DisplayName("test tryProcess when tapEvent is Dml event")
    @Test
    void test1(){
        BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = mock(BiConsumer.class);
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapUpdateRecordEvent tapUpdateRecordEvent = TapUpdateRecordEvent.create();
        tapUpdateRecordEvent.putAfterValue("name","wim");
        tapdataEvent.setTapEvent(tapUpdateRecordEvent);
        addDateFieldProcessNode.tryProcess(tapdataEvent,consumer);
        doAnswer(invocationOnMock -> {
            TapdataEvent tapdataEvent1 = (TapdataEvent) invocationOnMock.getArgument(0);
            TapUpdateRecordEvent tapEvent = (TapUpdateRecordEvent) tapdataEvent1.getTapEvent();
            Map<String, Object> after = tapEvent.getAfter();
            assertEquals(2,after.size());
            assertEquals(true,after.containsKey("dateTime"));
            assertEquals("wim",after.get("name"));
            return null;
        }).when(consumer).accept(any(),any());
    }
    @DisplayName("test tryProcess when tapEvent is Delete Event")
    @Test
    void test2(){
        BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = mock(BiConsumer.class);
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapDeleteRecordEvent tapDeleteRecordEvent= TapDeleteRecordEvent.create();
        Map<String,Object> before=new HashMap<>();
        before.put("name","wim");
        tapDeleteRecordEvent.setBefore(before);
        tapdataEvent.setTapEvent(tapDeleteRecordEvent);
        addDateFieldProcessNode.tryProcess(tapdataEvent,consumer);
        doAnswer(invocationOnMock -> {
            TapdataEvent tapdataEvent1 = (TapdataEvent) invocationOnMock.getArgument(0);
            TapUpdateRecordEvent tapEvent = (TapUpdateRecordEvent) tapdataEvent1.getTapEvent();
            Map<String, Object> after = tapEvent.getAfter();
            assertEquals(2,after.size());
            assertEquals(true,after.containsKey("dateTime"));
            assertEquals("wim",after.get("name"));
            return null;
        }).when(consumer).accept(any(),any());
    }
    @DisplayName("test tryProcess when tapEvent is not DML event")
    @Test
    void test3(){
        BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = mock(BiConsumer.class);
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapClearTableEvent tapClearTableEvent=new TapClearTableEvent();
        tapdataEvent.setTapEvent(tapClearTableEvent);
        addDateFieldProcessNode.tryProcess(tapdataEvent,consumer);
        doAnswer(invocationOnMock -> {
            TapdataEvent tapdataEvent1 = (TapdataEvent) invocationOnMock.getArgument(0);
            TapEvent tapEvent = tapdataEvent1.getTapEvent();
            boolean b = tapEvent instanceof TapClearTableEvent;
            assertEquals(true,b);
            return null;
        }).when(consumer).accept(any(),any());
    }

}
