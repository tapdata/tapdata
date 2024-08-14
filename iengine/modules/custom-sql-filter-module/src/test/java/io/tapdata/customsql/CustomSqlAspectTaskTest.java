package io.tapdata.customsql;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.core.api.ConnectorNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;


public class CustomSqlAspectTaskTest {
    @Test
    void test_handleStreamReadFunc(){
        try(MockedStatic<ConnectorNodeService> connectorNodeServiceMockedStatic = mockStatic(ConnectorNodeService.class)){
            ConnectorNodeService connectorNodeService = mock(ConnectorNodeService.class);
            connectorNodeServiceMockedStatic.when(ConnectorNodeService::getInstance).thenReturn(connectorNodeService);
            ConnectorNode connectorNode = mock(ConnectorNode.class);
            when(connectorNodeService.getConnectorNode(anyString())).thenReturn(connectorNode);
            TapCodecsFilterManager tapCodecsFilterManager = mock(TapCodecsFilterManager.class);
            when(connectorNode.getCodecsFilterManager()).thenReturn(tapCodecsFilterManager);
            StreamReadFuncAspect streamReadFuncAspect = spy(new StreamReadFuncAspect());
            doReturn(1).when(streamReadFuncAspect).getState();
            DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
            TableNode tableNode = mock(TableNode.class);
            when(dataProcessorContext.getNode()).thenReturn((Node) tableNode);
            when(dataProcessorContext.getPdkAssociateId()).thenReturn("test");
            when(tableNode.getIsFilter()).thenReturn(true);
            List<QueryOperator> queryOperatorList = new ArrayList<>();
            queryOperatorList.add(new QueryOperator());
            when(tableNode.getConditions()).thenReturn(queryOperatorList);
            doReturn(dataProcessorContext).when(streamReadFuncAspect).getDataProcessorContext();
            CustomSqlAspectTask customSqlAspectTask = spy(new CustomSqlAspectTask());
            customSqlAspectTask.handleStreamReadFunc(streamReadFuncAspect);
            List<TapdataEvent> tapdataEvents = new ArrayList<>();
            TapUpdateRecordEvent updateRecordEvent = new TapUpdateRecordEvent();
            Map<String,Object> after = new HashMap<>();
            after.put("_id","test");
            after.put("name","test");
            updateRecordEvent.setAfter(after);
            updateRecordEvent.setTableId("test");
            TapdataEvent tapdataEvent = new TapdataEvent();
            tapdataEvent.setTapEvent(updateRecordEvent);
            tapdataEvents.add(tapdataEvent);
            doReturn(false).when(customSqlAspectTask).checkAndFilter(any(),any(),any(),any());
            AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_PROCESS_COMPLETED).getStreamingProcessCompleteConsumers(), tapdataEvents);
            Assertions.assertInstanceOf(TapDeleteRecordEvent.class, tapdataEvents.get(0).getTapEvent());
        }
    }
}
