package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.StopEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.ddl.table.TapRenameTableEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;

import static org.mockito.Mockito.doCallRealMethod;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/5/21 15:38 Create
 */
class HazelcastRenameTableProcessorNodeTest extends BaseHazelcastNodeTest {

    private static final String tableName = "TEST";
    private static final String expected = tableName;
    private HazelcastRenameTableProcessorNode instance;

    @BeforeEach
    void beforeSetUp() {
        super.allSetup();

        TableRenameProcessNode node = new TableRenameProcessNode();
        ReflectionTestUtils.setField(processorBaseContext, "node", node);
        doCallRealMethod().when(processorBaseContext).getNode();

        instance = new HazelcastRenameTableProcessorNode(processorBaseContext);
    }

    @Test
    void testTryProcess() {
        TapInsertRecordEvent insertRecord = TapInsertRecordEvent.create().table(tableName);
        TapdataEvent event = new TapdataEvent();
        event.setTapEvent(insertRecord);

        instance.tryProcess(event, (tapdataEvent, processResult) -> {
            TapBaseEvent tapEvent = (TapBaseEvent) tapdataEvent.getTapEvent();
            Assertions.assertEquals(expected, tapEvent.getTableId(), String.format("Case '%s' to '%s'", tableName, expected));
        });

        // Verify un-TapBaseEvent has not exceptions.
        TapdataEvent tmpEvent = new TapdataEvent();
        tmpEvent.setTapEvent(new StopEvent());
        instance.tryProcess(tmpEvent, (tapdataEvent, processResult) -> {
        });

        TapdataEvent e = new TapdataEvent();
        TapCreateTableEvent createTableEvent = new TapCreateTableEvent();
        createTableEvent.setTableId("test_1");
        createTableEvent.setPartitionMasterTableId("test");
        e.setTapEvent(createTableEvent);
        instance.tryProcess(e, ((tapdataEvent, processResult) -> {
            Assertions.assertNotNull(tapdataEvent);
            Assertions.assertInstanceOf(TapCreateTableEvent.class, tapdataEvent.getTapEvent());
        }));
        Assertions.assertTrue(instance.supportConcurrentProcess());
        Assertions.assertFalse(instance.needTransformValue());
    }

    @Nested
    class GetTgtTableNameFromTapEventTest {
        @Test
        void testTableIUDAndRename() {

            // create table
            TapCreateTableEvent createTable = new TapCreateTableEvent();
            createTable.setTableId(tableName);
            String currentTableName = instance.getTgtTableNameFromTapEvent(createTable);
            Assertions.assertEquals(expected, currentTableName);

            // rename table
            TapRenameTableEvent tableEvent = new TapRenameTableEvent();
            tableEvent.setTableId(tableName);
            tableEvent.setNameChanges(Collections.singletonList(new ValueChange<>(tableName, expected)));
            currentTableName = instance.getTgtTableNameFromTapEvent(tableEvent);
            Assertions.assertEquals(expected, currentTableName);

            // drop table
            TapDropTableEvent dropTable = new TapDropTableEvent();
            dropTable.setTableId(tableName);
            currentTableName = instance.getTgtTableNameFromTapEvent(dropTable);
            Assertions.assertEquals(expected, currentTableName);

            // second stop table test tableName not in cache
            currentTableName = instance.getTgtTableNameFromTapEvent(dropTable);
            Assertions.assertEquals(expected, currentTableName);
        }
    }

}
