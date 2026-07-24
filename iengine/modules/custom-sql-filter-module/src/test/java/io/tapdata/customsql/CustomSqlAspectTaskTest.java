package io.tapdata.customsql;

import cn.hutool.core.map.MapUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataHeartbeatEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.schema.TapTableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;


public class CustomSqlAspectTaskTest {

    /**
     * 驱动 handleStreamReadFunc。
     *
     * @param afterMatch  checkAndFilter 的返回值，即 after 是否满足过滤条件
     * @param beforeMatch compareValue 的返回值，即 before 是否满足过滤条件；为 null 表示 before 为空、不会被调用
     */
    private void runFilter(List<TapdataEvent> events, boolean afterMatch, Boolean beforeMatch) {
        try (MockedStatic<ConnectorNodeService> connectorNodeServiceMockedStatic = mockStatic(ConnectorNodeService.class)) {
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
            TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("test", new TapTable("test"));
            when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
            doReturn(dataProcessorContext).when(streamReadFuncAspect).getDataProcessorContext();
            CustomSqlAspectTask customSqlAspectTask = spy(new CustomSqlAspectTask());
            doReturn(afterMatch).when(customSqlAspectTask).checkAndFilter(any(), any(), any(), any());
            if (beforeMatch != null) {
                doReturn(beforeMatch).when(customSqlAspectTask).compareValue(any(), any(), any(), any());
            }
            customSqlAspectTask.handleStreamReadFunc(streamReadFuncAspect);
            AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_PROCESS_COMPLETED).getStreamingProcessCompleteConsumers(), events);
        }
    }

    private TapdataEvent updateEvent(Map<String, Object> before, Map<String, Object> after) {
        TapUpdateRecordEvent updateRecordEvent = new TapUpdateRecordEvent();
        updateRecordEvent.setTableId("test");
        updateRecordEvent.setReferenceTime(System.currentTimeMillis());
        if (before != null) {
            updateRecordEvent.setBefore(before);
        }
        if (after != null) {
            updateRecordEvent.setAfter(after);
        }
        TapdataEvent tapdataEvent = new TapdataEvent();
        tapdataEvent.setTapEvent(updateRecordEvent);
        return tapdataEvent;
    }

    private Map<String, Object> row(Object id) {
        Map<String, Object> m = new HashMap<>();
        m.put("_id", id);
        m.put("name", "test");
        return m;
    }

    @Test
    void test_handleStreamReadFunc() {
        List<TapdataEvent> events = new ArrayList<>();
        events.add(updateEvent(null, row("test")));
        runFilter(events, false, null);
        Assertions.assertInstanceOf(TapDeleteRecordEvent.class, events.get(0).getTapEvent());
        TapDeleteRecordEvent tapDeleteRecordEvent = (TapDeleteRecordEvent) events.get(0).getTapEvent();
        Assertions.assertTrue(MapUtil.isNotEmpty(tapDeleteRecordEvent.getBefore()));
    }

    @Test
    void test_keepUpdate_whenNotCrossBoundary() {
        List<TapdataEvent> events = new ArrayList<>();
        events.add(updateEvent(row("a"), row("a")));
        runFilter(events, true, true);
        Assertions.assertEquals(1, events.size());
        Assertions.assertInstanceOf(TapUpdateRecordEvent.class, events.get(0).getTapEvent());
    }

    @Test
    void test_toInsert_whenEnterFilterSet() {
        List<TapdataEvent> events = new ArrayList<>();
        events.add(updateEvent(row("a"), row("a")));
        runFilter(events, true, false);
        Assertions.assertEquals(1, events.size());
        Assertions.assertInstanceOf(TapInsertRecordEvent.class, events.get(0).getTapEvent());
    }

    @Test
    void test_toDelete_whenLeaveFilterSet() {
        List<TapdataEvent> events = new ArrayList<>();
        Map<String, Object> before = row("a");
        events.add(updateEvent(before, row("a")));
        runFilter(events, false, true);
        Assertions.assertEquals(1, events.size());
        Assertions.assertInstanceOf(TapDeleteRecordEvent.class, events.get(0).getTapEvent());
        TapDeleteRecordEvent deleteEvent = (TapDeleteRecordEvent) events.get(0).getTapEvent();
        Assertions.assertEquals(before, deleteEvent.getBefore());
    }

    @Test
    void test_drop_whenAlwaysOutOfFilterSet() {
        List<TapdataEvent> events = new ArrayList<>();
        events.add(updateEvent(row("a"), row("a")));
        runFilter(events, false, false);
        Assertions.assertEquals(1, events.size());
        Assertions.assertInstanceOf(TapdataHeartbeatEvent.class, events.get(0));
    }

    private TapTableMap<String, TapTable> tableMapWith(TapField field) {
        TapTable tapTable = new TapTable("test");
        tapTable.add(field);
        return TapTableMap.create("test", tapTable);
    }

    private QueryOperator op(String key, Object value, int operator) {
        QueryOperator queryOperator = new QueryOperator();
        queryOperator.setKey(key);
        queryOperator.setValue(value);
        queryOperator.setOperator(operator);
        return queryOperator;
    }

    private TapdataEvent deleteEvent(Map<String, Object> before) {
        TapDeleteRecordEvent del = new TapDeleteRecordEvent();
        del.setTableId("test");
        del.setBefore(before);
        TapdataEvent ev = new TapdataEvent();
        ev.setTapEvent(del);
        return ev;
    }

    @Test
    void real_delete_numberSatisfying_isKept() {
        CustomSqlAspectTask task = new CustomSqlAspectTask();
        TapTableMap<String, TapTable> tapTableMap = tableMapWith(new TapField("age", "INT").tapType(new TapNumber()));
        TableNode tableNode = mock(TableNode.class);
        when(tableNode.getConditions()).thenReturn(Collections.singletonList(op("age", 18, QueryOperator.GT)));
        TapCodecsFilterManager mgr = TapCodecsFilterManager.create(TapCodecsRegistry.create());
        Map<String, Object> before = new HashMap<>();
        before.put("age", 20);
        boolean result = task.checkAndFilter(deleteEvent(before), tapTableMap, tableNode, mgr);
        Assertions.assertTrue(result, "age=20 满足 age>18，删除应被保留(result=true)");
    }

    @Test
    void real_delete_filterFieldNull() {
        CustomSqlAspectTask task = new CustomSqlAspectTask();
        TapTableMap<String, TapTable> tapTableMap = tableMapWith(new TapField("status", "VARCHAR").tapType(new TapString()));
        TableNode tableNode = mock(TableNode.class);
        when(tableNode.getConditions()).thenReturn(Collections.singletonList(op("status", "active", 5)));
        TapCodecsFilterManager mgr = TapCodecsFilterManager.create(TapCodecsRegistry.create());
        Map<String, Object> before = new HashMap<>();
        before.put("_id", 1);
        before.put("status", null);
        boolean result = task.checkAndFilter(deleteEvent(before), tapTableMap, tableNode, mgr);
        System.out.println("[DIAG] delete before={_id:1, status:null}, condition status='active' -> checkAndFilter=" + result);
    }

    @Test
    void real_delete_filterFieldMissing() {
        CustomSqlAspectTask task = new CustomSqlAspectTask();
        TapTableMap<String, TapTable> tapTableMap = tableMapWith(new TapField("status", "VARCHAR").tapType(new TapString()));
        TableNode tableNode = mock(TableNode.class);
        when(tableNode.getConditions()).thenReturn(Collections.singletonList(op("status", "active", 5)));
        TapCodecsFilterManager mgr = TapCodecsFilterManager.create(TapCodecsRegistry.create());
        Map<String, Object> before = new HashMap<>();
        before.put("_id", 1);
        boolean result = task.checkAndFilter(deleteEvent(before), tapTableMap, tableNode, mgr);
        System.out.println("[DIAG] delete before={_id:1} (no filter field), condition status='active' -> checkAndFilter=" + result);
    }

    @Test
    void real_delete_stringEqlSatisfying_isKept() {
        CustomSqlAspectTask task = new CustomSqlAspectTask();
        TapTableMap<String, TapTable> tapTableMap = tableMapWith(new TapField("status", "VARCHAR").tapType(new TapString()));
        TableNode tableNode = mock(TableNode.class);
        when(tableNode.getConditions()).thenReturn(Collections.singletonList(op("status", "active", 5)));
        TapCodecsFilterManager mgr = TapCodecsFilterManager.create(TapCodecsRegistry.create());
        Map<String, Object> before = new HashMap<>();
        before.put("status", "active");
        boolean result = task.checkAndFilter(deleteEvent(before), tapTableMap, tableNode, mgr);
        Assertions.assertTrue(result, "status=active 满足 status='active'，删除应被保留(result=true)");
    }
}
