package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.*;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.Record;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import org.junit.jupiter.api.Assertions;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class RecordEventExecute {
    ConnectorNode connectorNode;
    PDKTestBase base;
    Method testCase;

    public Method testCase() {
        return this.testCase;
    }

    public RecordEventExecute testCase(Method testCase) {
        this.testCase = testCase;
        return this;
    }

    private ConnectorFunctions connectorFunctions;
    private WriteRecordFunction writeRecordFunction;

    public static RecordEventExecute create(ConnectorNode connectorNode, PDKTestBase base) {
        return new RecordEventExecute(connectorNode, base);
    }

    private RecordEventExecute(ConnectorNode connectorNode, PDKTestBase base) {
        this.connectorNode = connectorNode;
        this.base = base;

        connectorFunctions = connectorNode.getConnectorFunctions();
        writeRecordFunction = connectorFunctions.getWriteRecordFunction();
    }

    List<Record> records = new ArrayList<>();

    public RecordEventExecute builderRecord(Record... records) {
        if (null == records || records.length <= 0) return this;
        if (records == null) this.records = new ArrayList<>();
        this.records.addAll(Arrays.asList(records));
        return this;
    }

    public RecordEventExecute resetRecords() {
        this.records = new ArrayList<>();
        return this;
    }

    public WriteListResult<TapRecordEvent> insert() throws Throwable {
        List<TapRecordEvent> tapInsertRecordEvents = new ArrayList<>();
        records.forEach(record -> {
            TapInsertRecordEvent insertRecordEvent = new TapInsertRecordEvent().table(base.getTargetTable().getId());
            insertRecordEvent.setAfter(record);
            insertRecordEvent.setReferenceTime(System.currentTimeMillis());
            tapInsertRecordEvents.add(insertRecordEvent);
        });
        AtomicReference<WriteListResult<TapRecordEvent>> consumerBack = new AtomicReference<>(new WriteListResult<TapRecordEvent>());
        writeRecordFunction.writeRecord(
                connectorNode.getConnectorContext(),
                tapInsertRecordEvents,
                base.getTargetTable(),
                consumer -> {
                    if (null != consumer) consumerBack.set(consumer);
                }
        );
        return consumerBack.get();
    }

    public WriteListResult<TapRecordEvent> update() throws Throwable {
        List<TapRecordEvent> tapUpdateRecordEvents = new ArrayList<>();
        records.forEach(record -> {
            TapUpdateRecordEvent updateRecordEvent = new TapUpdateRecordEvent().table(base.getTargetTable().getId());
            updateRecordEvent.setAfter(record);
            updateRecordEvent.setReferenceTime(System.currentTimeMillis());
            tapUpdateRecordEvents.add(updateRecordEvent);
        });
        AtomicReference<WriteListResult<TapRecordEvent>> consumerBack = new AtomicReference<>();
        writeRecordFunction.writeRecord(
                connectorNode.getConnectorContext(),
                tapUpdateRecordEvents,
                base.getTargetTable(),
                consumer -> {
                    if (null != consumer) consumerBack.set(consumer);
                }
        );
        return consumerBack.get();
    }

    public WriteListResult<TapRecordEvent> delete() throws Throwable {
        List<TapRecordEvent> tapDeleteRecordEvents = new ArrayList<>();
        records.forEach(record -> {
            TapDeleteRecordEvent deleteRecordEvent = new TapDeleteRecordEvent().table(base.getTargetTable().getId());
            deleteRecordEvent.setBefore(record);
            deleteRecordEvent.setReferenceTime(System.currentTimeMillis());
            tapDeleteRecordEvents.add(deleteRecordEvent);
        });
        AtomicReference<WriteListResult<TapRecordEvent>> consumerBack = new AtomicReference<>();
        writeRecordFunction.writeRecord(
                connectorNode.getConnectorContext(),
                tapDeleteRecordEvents,
                base.getTargetTable(),
                consumer -> {
                    if (null != consumer)
                        consumerBack.set(consumer);
                }
        );
        return consumerBack.get();
    }

    /**
     * @deprecated
     */
    public boolean createTable() throws Throwable {
        CreateTableV2Function createTable = connectorFunctions.getCreateTableV2Function();
        CreateTableFunction createTableFunction = connectorFunctions.getCreateTableFunction();
        Assertions.assertTrue(null == createTable || null == createTableFunction, "%{please_support_create_table_function}%");
        TapCreateTableEvent createTableEvent = new TapCreateTableEvent().table(base.getTargetTable());
        if (null != createTable) {
            CreateTableOptions table = createTable.createTable(connectorNode.getConnectorContext(), createTableEvent);
            Assertions.assertNull(table, "%{null_after_create_table}%");
            Assertions.assertTrue(table.getTableExists(), "%{create_table_table_not_exists}%");
            return Boolean.TRUE;
        }
        if (null != createTableFunction) {
            createTableFunction.createTable(connectorNode.getConnectorContext(), createTableEvent);
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    public void dropTable() {
        TapAssert.asserts(
                () -> Assertions.assertDoesNotThrow(this::drop, LangUtil.format("recordEventExecute.drop.table.error"))
        ).acceptAsWarn(testCase, LangUtil.format("recordEventExecute.drop.notCatch.thrower"));
    }

    private boolean drop() throws Throwable {
        DropTableFunction dropTableFunction = connectorFunctions.getDropTableFunction();
        if (null == dropTableFunction) {
            TapAssert.asserts(() -> Assertions.fail(LangUtil.format("base.notSupportDropTable", base.getTargetTable().getId()))).warn(testCase);
            return false;
        }
        TapAssert.asserts(
                () -> Assertions.assertNotNull(dropTableFunction, LangUtil.format("recordEventExecute.drop.error.not.support.function"))
        ).acceptAsError(testCase, LangUtil.format("recordEventExecute.drop.table.succeed", base.getTargetTable().getId()));
        TapDropTableEvent dropTableEvent = new TapDropTableEvent();
        dropTableEvent.setTableId(base.getTargetTable().getId());
        dropTableEvent.setReferenceTime(System.currentTimeMillis());
        dropTableFunction.dropTable(connectorNode.getConnectorContext(), dropTableEvent);
        return Boolean.TRUE;
    }
}
