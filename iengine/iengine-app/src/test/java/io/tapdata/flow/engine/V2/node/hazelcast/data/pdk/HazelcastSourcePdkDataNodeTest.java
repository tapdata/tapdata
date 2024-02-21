package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

@DisplayName("HazelcastSourcePdkDataNode Class Test")
public class HazelcastSourcePdkDataNodeTest extends BaseHazelcastNodeTest {

    private HazelcastSourcePdkDataNode hazelcastSourcePdkDataNode;

    @BeforeEach
    void beforeEach() {
        hazelcastSourcePdkDataNode = mock(HazelcastSourcePdkDataNode.class);
        doCallRealMethod().when(hazelcastSourcePdkDataNode).flushPollingCDCOffset(anyList());
    }

    @Test
    @SneakyThrows
    @DisplayName("main process test")
    void testMainProcess() {
        List<TapEvent> tapEvents = new ArrayList<>();
        tapEvents.add(new TapInsertRecordEvent().after(new HashMap<>()));
        tapEvents.add(new TapUpdateRecordEvent().after(new HashMap<>()));
        tapEvents.add(new TapDropFieldEvent().fieldName("test"));
        hazelcastSourcePdkDataNode.flushPollingCDCOffset(tapEvents);
        verify(hazelcastSourcePdkDataNode, times(2)).getConnectorNode();
    }
}
