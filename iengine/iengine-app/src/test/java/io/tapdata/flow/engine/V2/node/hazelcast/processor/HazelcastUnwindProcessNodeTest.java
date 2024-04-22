package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.ArrayModel;
import com.tapdata.tm.commons.dag.UnwindModel;
import com.tapdata.tm.commons.dag.process.UnwindProcessNode;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.*;

public class HazelcastUnwindProcessNodeTest extends BaseHazelcastNodeTest {
    HazelcastUnwindProcessNode hazelcastUnwindProcessNode ;
    @BeforeEach
    void beforeSetUp(){
        super.allSetup();
        hazelcastUnwindProcessNode = new HazelcastUnwindProcessNode(processorBaseContext);

    }
    @Test
    void tryProcessTest(){
        UnwindProcessNode unwindProcessNode = new UnwindProcessNode();
        unwindProcessNode.setUnwindModel(UnwindModel.FLATTEN);
        unwindProcessNode.setArrayModel(ArrayModel.OBJECT);
        unwindProcessNode.setPath("t");
        unwindProcessNode.setPreserveNullAndEmptyArrays(false);
        ReflectionTestUtils.setField(hazelcastUnwindProcessNode,"node",unwindProcessNode);
        TapdataEvent tapdataEvent =new TapdataEvent();
        TapInsertRecordEvent tapInsertRecordEvent  = TapInsertRecordEvent.create();
        Map<String,Object> after = new HashMap<>();
        after.put("id","test");
        after.put("t",null);
        tapInsertRecordEvent.setAfter(after);
        tapdataEvent.setTapEvent(tapInsertRecordEvent);
        BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = mock(BiConsumer.class);
        hazelcastUnwindProcessNode.tryProcess(tapdataEvent,consumer);
        verify(consumer,times(1)).accept(any(),any());
    }

    @Test
    void tryProcessTest_afterIsNull(){
        UnwindProcessNode unwindProcessNode = new UnwindProcessNode();
        unwindProcessNode.setUnwindModel(UnwindModel.FLATTEN);
        unwindProcessNode.setArrayModel(ArrayModel.OBJECT);
        unwindProcessNode.setPath("t");
        unwindProcessNode.setPreserveNullAndEmptyArrays(false);
        ReflectionTestUtils.setField(hazelcastUnwindProcessNode,"node",unwindProcessNode);
        TapdataEvent tapdataEvent =new TapdataEvent();
        TapInsertRecordEvent tapInsertRecordEvent  = TapInsertRecordEvent.create();
        tapInsertRecordEvent.setAfter(null);
        tapdataEvent.setTapEvent(tapInsertRecordEvent);
        BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = mock(BiConsumer.class);
        hazelcastUnwindProcessNode.tryProcess(tapdataEvent,consumer);
        verify(consumer,times(0)).accept(any(),any());
    }

    @Test
    void tryProcessTest_ArrayModelIsNullObject(){
        UnwindProcessNode unwindProcessNode = new UnwindProcessNode();
        unwindProcessNode.setUnwindModel(UnwindModel.FLATTEN);
        unwindProcessNode.setArrayModel(ArrayModel.MIX);
        unwindProcessNode.setPath("t");
        unwindProcessNode.setPreserveNullAndEmptyArrays(false);
        ReflectionTestUtils.setField(hazelcastUnwindProcessNode,"node",unwindProcessNode);
        TapdataEvent tapdataEvent =new TapdataEvent();
        TapInsertRecordEvent tapInsertRecordEvent  = TapInsertRecordEvent.create();
        Map<String,Object> after = new HashMap<>();
        after.put("id","test");
        after.put("t",null);
        tapInsertRecordEvent.setAfter(after);
        tapdataEvent.setTapEvent(tapInsertRecordEvent);
        BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = mock(BiConsumer.class);
        hazelcastUnwindProcessNode.tryProcess(tapdataEvent,consumer);
        verify(consumer,times(1)).accept(any(),any());
    }


}
