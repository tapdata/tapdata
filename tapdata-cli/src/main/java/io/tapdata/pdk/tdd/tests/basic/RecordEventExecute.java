package io.tapdata.pdk.tdd.tests.basic;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class RecordEventExecute {
    ConnectorNode connectorNode;
    PDKTestBase base;
    Method testCase;
    Map<String, Object> tddConfig;

    public Map<String, Object> code(Map<String,Object> map){
        LinkedHashMap<String, TapField> nameFieldMap = base.getTargetTable(connectorNode).getNameFieldMap();
        TapCodecsFilterManager codecsFilterManager = new TapCodecsFilterManager(TapCodecsRegistry.create());
        codecsFilterManager.transformToTapValueMap(map,nameFieldMap);
        TapCodecsFilterManager targetCodecsFilterManager = connectorNode.getCodecsFilterManager();
        targetCodecsFilterManager.transformFromTapValueMap(map, nameFieldMap);
        return map;
    }
    public Method testCase() {
        return this.testCase;
    }

    public RecordEventExecute testCase(Method testCase) {
        this.testCase = testCase;
        return this;
    }

    public Map<String, Object> tddConfig() {
        return this.tddConfig;
    }

    public RecordEventExecute tddConfig(Map<String, Object> tddConfig) {
        this.tddConfig = tddConfig;
        return this;
    }

    public Object findTddConfig(String key) {
        if (Objects.isNull(this.tddConfig) || tddConfig.isEmpty()) return null;
        return this.tddConfig.get(key);
    }

    public <T> T findTddConfig(String key, Class<T> type) {
        if (Objects.isNull(this.tddConfig) || tddConfig.isEmpty()) return null;
        Object obj = this.tddConfig.get(key);
        if (Objects.isNull(obj)) return null;
        try {
            return (T) this.tddConfig.get(key);
        } catch (Exception e) {
            return null;
        }
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

    public RecordEventExecute builderRecordCleanBefore(Record... records) {
        this.records = new ArrayList<>();
        for (Record record : records) {
            Record map = Record.create();
            map.putAll(this.code(InstanceFactory.instance(TapUtils.class).cloneMap(record)));
            this.records.add(map);
        }
        return this;
    }
    public Record[] records(){
        return this.records.toArray(new Record[0]);
    }

    public RecordEventExecute builderRecord(Record... records) {
        if (null == records || records.length <= 0) return this;
        if (this.records == null) this.records = new ArrayList<>();
        for (Record record : records) {
            Record map = Record.create();
            map.putAll(this.code(InstanceFactory.instance(TapUtils.class).cloneMap(record)));
            this.records.add(map);
        }
        return this;
    }

    public RecordEventExecute resetRecords() {
        this.records = new ArrayList<>();
        return this;
    }

    public WriteListResult<TapRecordEvent> insert() throws Throwable {
        return this.insert(this.records);
    }

    public WriteListResult<TapRecordEvent> update() throws Throwable {
        return this.update(this.records);
    }

    public WriteListResult<TapRecordEvent> delete() {
        try {
            return this.deletes(this.records);
        }catch (Throwable t){
            throw new RuntimeException(t.getMessage());
        }
    }

    public WriteListResult<TapRecordEvent> insert(Record[] records) throws Throwable {
        return this.insert(this.recordsAsList(records));
    }

    public WriteListResult<TapRecordEvent> update(Record[] records) throws Throwable {
        return this.update(this.recordsAsList(records));
    }

    public WriteListResult<TapRecordEvent> deletes(Record[] records) throws Throwable {
        return this.deletes(this.recordsAsList(records));
    }

    public List<Record> recordsAsList(Record[] records) {
        List<Record> recordList = new ArrayList<>();
        if (null == records || records.length <= 0) return recordList;
        recordList.addAll(Arrays.asList(records));
        return recordList;
    }

    public WriteListResult<TapRecordEvent> insert(List<Record> records) throws Throwable {
        List<TapRecordEvent> tapInsertRecordEvents = new ArrayList<>();
        records.forEach(record -> {
            TapInsertRecordEvent insertRecordEvent = new TapInsertRecordEvent().table(base.getSourceTable().getId());
            insertRecordEvent.setAfter(record);
            insertRecordEvent.setReferenceTime(System.currentTimeMillis());
            tapInsertRecordEvents.add(insertRecordEvent);
        });
        AtomicReference<WriteListResult<TapRecordEvent>> consumerBack = new AtomicReference<>(new WriteListResult<TapRecordEvent>());
        writeRecordFunction.writeRecord(
                connectorNode.getConnectorContext(),
                tapInsertRecordEvents,
                base.getSourceTable(),
                consumer -> {
                    if (null != consumer) consumerBack.set(consumer);
                }
        );
        return consumerBack.get();
    }

    public WriteListResult<TapRecordEvent> update(List<Record> records) throws Throwable {
        List<TapRecordEvent> tapUpdateRecordEvents = new ArrayList<>();
        records.forEach(record -> {
            TapUpdateRecordEvent updateRecordEvent = new TapUpdateRecordEvent().table(base.getSourceTable().getId());
            updateRecordEvent.setAfter(record);
            updateRecordEvent.setReferenceTime(System.currentTimeMillis());
            tapUpdateRecordEvents.add(updateRecordEvent);
        });
        AtomicReference<WriteListResult<TapRecordEvent>> consumerBack = new AtomicReference<>();
        writeRecordFunction.writeRecord(
                connectorNode.getConnectorContext(),
                tapUpdateRecordEvents,
                base.getSourceTable(),
                consumer -> {
                    if (null != consumer) consumerBack.set(consumer);
                }
        );
        return consumerBack.get();
    }

    public WriteListResult<TapRecordEvent> deletes(List<Record> records) throws Throwable {
        List<TapRecordEvent> tapDeleteRecordEvents = new ArrayList<>();
        records.forEach(record -> {
            TapDeleteRecordEvent deleteRecordEvent = new TapDeleteRecordEvent().table(base.getSourceTable().getId());
            deleteRecordEvent.setBefore(record);
            deleteRecordEvent.setReferenceTime(System.currentTimeMillis());
            tapDeleteRecordEvents.add(deleteRecordEvent);
        });
        AtomicReference<WriteListResult<TapRecordEvent>> consumerBack = new AtomicReference<>();
        writeRecordFunction.writeRecord(
                connectorNode.getConnectorContext(),
                tapDeleteRecordEvents,
                base.getSourceTable(),
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
        TapCreateTableEvent createTableEvent = new TapCreateTableEvent().table(base.getSourceTable());
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
        this.drop(this.base.getSourceTable());
    }

    public void dropTable(TapTable tapTable) {
        if (Objects.nonNull(tapTable)) {
            this.drop(tapTable);
        } else {
            TapAssert.warn(this.testCase, "Empty table cannot drop.");
        }
    }

    private boolean drop(TapTable tapTable) {
        DropTableFunction dropTableFunction = this.connectorFunctions.getDropTableFunction();
        if (null == dropTableFunction) {
            TapAssert.asserts(() -> Assertions.fail(LangUtil.format("base.notSupportDropTable", tapTable.getId()))).warn(this.testCase);
            return false;
        }
        TapAssert.asserts(
                () -> Assertions.assertNotNull(dropTableFunction, LangUtil.format("recordEventExecute.drop.error.not.support.function"))
        ).acceptAsError(this.testCase, LangUtil.format("recordEventExecute.drop.table.succeed", tapTable.getId()));
        TapDropTableEvent dropTableEvent = new TapDropTableEvent();
        dropTableEvent.setTableId(tapTable.getId());
        dropTableEvent.setReferenceTime(System.currentTimeMillis());
        try {
            dropTableFunction.dropTable(this.connectorNode.getConnectorContext(), dropTableEvent);
            TapAssert.succeed(this.testCase, LangUtil.format("recordEventExecute.drop.notCatch.thrower"));
        } catch (Throwable e) {
            TapAssert.warn(this.testCase, LangUtil.format("recordEventExecute.drop.table.error", e.getMessage()));
        }
        return Boolean.TRUE;
    }
}
