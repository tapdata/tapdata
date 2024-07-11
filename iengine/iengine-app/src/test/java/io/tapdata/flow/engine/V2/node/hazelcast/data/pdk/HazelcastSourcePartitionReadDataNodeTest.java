package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataHeartbeatEvent;
import com.tapdata.tm.commons.cdcdelay.CdcDelay;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

public class HazelcastSourcePartitionReadDataNodeTest extends BaseHazelcastNodeTest {
    private HazelcastSourcePartitionReadDataNode hazelcastSourcePartitionReadDataNode;
    private CdcDelay cdcDelay;
    StreamReadFuncAspect streamReadFuncAspect;


    @BeforeEach
    void setUp() {
        super.allSetup();
        Logger logger = mock(Logger.class);
        cdcDelay = mock(CdcDelay.class);
        hazelcastSourcePartitionReadDataNode = mock(HazelcastSourcePartitionReadDataNode.class);

        ReflectionTestUtils.setField(hazelcastSourcePartitionReadDataNode, "logger", logger);
        ReflectionTestUtils.setField(hazelcastSourcePartitionReadDataNode, "cdcDelayCalculation", cdcDelay);
        ReflectionTestUtils.setField(hazelcastSourcePartitionReadDataNode, "dataProcessorContext", dataProcessorContext);

    }

    @DisplayName("test filterAndCalcDelay table is heartBeat")
    @Test
    void test1() {
        streamReadFuncAspect=new StreamReadFuncAspect();
        ReflectionTestUtils.setField(hazelcastSourcePartitionReadDataNode, "streamReadFuncAspect", streamReadFuncAspect);
        List<TapEvent> tapEvents = new ArrayList<>();
        TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
        tapUpdateRecordEvent.setTableId("testTableId");
        tapUpdateRecordEvent.setTime(System.currentTimeMillis());
        tapEvents.add(tapUpdateRecordEvent);
        doCallRealMethod().when(hazelcastSourcePartitionReadDataNode).handleStreamEventsReceived(tapEvents, null);

        List<TapdataEvent> tapdataEvents = new ArrayList<>();
        TapdataHeartbeatEvent heartbeatEvent = new TapdataHeartbeatEvent();
        tapdataEvents.add(heartbeatEvent);

        when(hazelcastSourcePartitionReadDataNode.wrapTapdataEvent(any(), any(), any())).thenReturn(tapdataEvents);
        HeartbeatEvent heartbeatEvent2 = new HeartbeatEvent();
        TapEvent tapEvent = tapUpdateRecordEvent;
        tapEvent.clone(heartbeatEvent2);
        when(cdcDelay.filterAndCalcDelay(tapEvent, null, null)).thenReturn(heartbeatEvent2);
        hazelcastSourcePartitionReadDataNode.handleStreamEventsReceived(tapEvents, null);
        verify(cdcDelay, times(1)).filterAndCalcDelay(any(), any(), any());
    }
    @DisplayName("test streamReadFuncAspect is null ")
    @Test
    void test2(){
        List<TapEvent> tapEvents = new ArrayList<>();
        TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
        tapUpdateRecordEvent.setTableId("testId");
        tapUpdateRecordEvent.setTime(System.currentTimeMillis());
        tapEvents.add(tapUpdateRecordEvent);
        doCallRealMethod().when(hazelcastSourcePartitionReadDataNode).handleStreamEventsReceived(tapEvents, null);

        List<TapdataEvent> tapdataEvents = new ArrayList<>();
        TapdataHeartbeatEvent heartbeatEvent = new TapdataHeartbeatEvent();
        tapdataEvents.add(heartbeatEvent);

        when(hazelcastSourcePartitionReadDataNode.wrapTapdataEvent(any(), any(), any())).thenReturn(tapdataEvents);
        HeartbeatEvent hbEvent = new HeartbeatEvent();
        TapEvent tapEvent = tapUpdateRecordEvent;
        tapEvent.clone(hbEvent);
        when(cdcDelay.filterAndCalcDelay(tapEvent, null, null)).thenReturn(hbEvent);
        hazelcastSourcePartitionReadDataNode.handleStreamEventsReceived(tapEvents, null);
        verify(cdcDelay, times(1)).filterAndCalcDelay(any(), any(), any());
    }


}
