package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.*;
import io.tapdata.pdk.cli.commands.TDDPrintf;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.tests.support.LangUtils;
import io.tapdata.pdk.tdd.tests.support.Record;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import org.junit.jupiter.api.Assertions;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

public class RecordEventExecute {
    ConnectorNode connectorNode;
    PDKTestBase base;
    Method testCase;
    public RecordEventExecute testCase(Method testCase){
        this.testCase = testCase;
        return this;
    }
    private ConnectorFunctions connectorFunctions ;
    private WriteRecordFunction writeRecordFunction ;
    private TapTable targetTable ;
    private TapConnectorContext connectionContext;
    public static RecordEventExecute create(ConnectorNode connectorNode, TapConnectorContext connectionContext, PDKTestBase base){
        return new RecordEventExecute(connectorNode,connectionContext,base);
    }
    private RecordEventExecute(ConnectorNode connectorNode, TapConnectorContext connectionContext,PDKTestBase base){
        this.connectorNode = connectorNode;
        this.base = base;
        this.connectionContext = connectionContext;

        connectorFunctions = connectorNode.getConnectorFunctions();
        writeRecordFunction = connectorFunctions.getWriteRecordFunction();
        targetTable = connectorNode.getConnectorContext().getTableMap().get(connectorNode.getTable());
    }

    List<Record> records = new ArrayList<>();
    public RecordEventExecute builderRecord(Record ... records){
        if (null == records || records.length<=0) return this;
        if (records == null) this.records = new ArrayList<Record>();
        for (Record record : records) {
            this.records.add(record);
        }
        return this;
    }
    public RecordEventExecute resetRecords(){
        this.records = new ArrayList<>();
        return this;
    }

    public WriteListResult<TapRecordEvent> insert() throws Throwable {
        List<TapRecordEvent> tapInsertRecordEvents = new ArrayList<>();
        records.forEach(record -> {
            TapInsertRecordEvent insertRecordEvent = new TapInsertRecordEvent().table(targetTable.getId());
            insertRecordEvent.setAfter(record);
            insertRecordEvent.setReferenceTime(System.currentTimeMillis());
            tapInsertRecordEvents.add(insertRecordEvent);
        });
        AtomicReference<WriteListResult<TapRecordEvent>> consumerBack = new AtomicReference<>();
        writeRecordFunction.writeRecord(
                connectorNode.getConnectorContext(),
                tapInsertRecordEvents,
                targetTable,
                consumer -> {
                    base.$(() ->
//                            TapAssert.asserts(()->
//                                Assertions.assertEquals(
//                                        tapInsertRecordEvents.size(),
//                                        consumer.getInsertedCount(),
//                                        "%{error_insert}% "+tapInsertRecordEvents.size()+" %{record_into_mongodb}%")
                           TapAssert.asserts(()->
                                Assertions.assertEquals(
                                        tapInsertRecordEvents.size(),
                                        consumer.getInsertedCount(),
                                        TDDPrintf.format("RecordEventExecute.insert.assert.error", tapInsertRecordEvents.size()))
                            ).acceptAsWarn(base.getClass(),testCase,TDDPrintf.format("RecordEventExecute.insert.assert.succeed",tapInsertRecordEvents.size()) )
                    );
                    consumerBack.set(consumer);
                }
        );
        return consumerBack.get();
    }

    public WriteListResult<TapRecordEvent> update() throws Throwable {
        List<TapRecordEvent> tapUpdateRecordEvents = new ArrayList<>();
        records.forEach(record -> {
            TapUpdateRecordEvent updateRecordEvent = new TapUpdateRecordEvent().table(targetTable.getId());
            updateRecordEvent.setAfter(record);
            updateRecordEvent.setReferenceTime(System.currentTimeMillis());
            tapUpdateRecordEvents.add(updateRecordEvent);
        });
        AtomicReference<WriteListResult<TapRecordEvent>> consumerBack = new AtomicReference<>();
        writeRecordFunction.writeRecord(
                connectorNode.getConnectorContext(),
                tapUpdateRecordEvents,
                targetTable,
                consumer -> {
                    base.$(() -> TapAssert.asserts(()->Assertions.assertEquals(
                                tapUpdateRecordEvents.size(),
                                consumer.getModifiedCount(),
                                TDDPrintf.format("RecordEventExecute.update.assert.error",tapUpdateRecordEvents.size()))
                            ).acceptAsError(base.getClass(),testCase,TDDPrintf.format("RecordEventExecute.update.assert.succeed",tapUpdateRecordEvents.size()))
                    );
                    consumerBack.set(consumer);
//                    this.dropTable(cla);
                }
        );
        return consumerBack.get();
    }

    public WriteListResult<TapRecordEvent> delete() throws Throwable {
        List<TapRecordEvent> tapDeleteRecordEvents = new ArrayList<>();
        records.forEach(record -> {
            TapDeleteRecordEvent deleteRecordEvent = new TapDeleteRecordEvent().table(targetTable.getId());
            deleteRecordEvent.setBefore(record);
            deleteRecordEvent.setReferenceTime(System.currentTimeMillis());
            tapDeleteRecordEvents.add(deleteRecordEvent);
        });
        AtomicReference<WriteListResult<TapRecordEvent>> consumerBack = new AtomicReference<>();
        writeRecordFunction.writeRecord(
                connectorNode.getConnectorContext(),
                tapDeleteRecordEvents,
                targetTable,
                consumer -> {
                    base.$(() -> TapAssert.asserts(()->Assertions.assertEquals(
                            tapDeleteRecordEvents.size(),
                            consumer.getRemovedCount(),
                            TDDPrintf.format("RecordEventExecute.delete.assert.error",tapDeleteRecordEvents.size()))
                            ).acceptAsError(base.getClass(),testCase,TDDPrintf.format("RecordEventExecute.delete.assert.succeed",tapDeleteRecordEvents.size()))
                    );
                    consumerBack.set(consumer);
//                    this.dropTable(cla);
                }
        );
        return consumerBack.get();
    }

    public boolean createTable() throws Throwable {
        CreateTableV2Function createTable = connectorFunctions.getCreateTableV2Function();
        CreateTableFunction createTableFunction = connectorFunctions.getCreateTableFunction();
        Assertions.assertTrue(null == createTable || null == createTableFunction,"%{please_support_create_table_function}%");
        TapCreateTableEvent createTableEvent = new TapCreateTableEvent().table(targetTable);
        if (null != createTable){
            CreateTableOptions table = createTable.createTable(connectionContext, createTableEvent);
            Assertions.assertNull(table,"%{null_after_create_table}%");
            Assertions.assertTrue(table.getTableExists(),"%{create_table_table_not_exists}%");
            return Boolean.TRUE;
        }
        if (null != createTableFunction){
            createTableFunction.createTable(connectionContext, createTableEvent);
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    public void dropTable() {
        TapAssert.asserts(
                ()->Assertions.assertDoesNotThrow(this::drop,TDDPrintf.format("RecordEventExecute.drop.table.error"))
        ).acceptAsError(base.getClass(),testCase,TDDPrintf.format("RecordEventExecute.drop.notCatch.thrower"));
    }

    private boolean drop() throws Throwable {
        DropTableFunction dropTableFunction = connectorFunctions.getDropTableFunction();
        TapAssert.asserts(
                ()->Assertions.assertNotNull(dropTableFunction,TDDPrintf.format("RecordEventExecute.drop.error.not.support.function"))
        ).acceptAsError(base.getClass(),testCase,TDDPrintf.format("RecordEventExecute.drop.table.succeed",targetTable.getId()));
        TapDropTableEvent dropTableEvent = new TapDropTableEvent();
        dropTableEvent.setTableId(targetTable.getId());
        dropTableEvent.setReferenceTime(System.currentTimeMillis());
        dropTableFunction.dropTable(connectionContext,dropTableEvent);
        return Boolean.TRUE;
    }

}
