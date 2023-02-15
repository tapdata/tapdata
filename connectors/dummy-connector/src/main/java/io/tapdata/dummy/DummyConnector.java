package io.tapdata.dummy;

import io.tapdata.base.ConnectorBase;
import io.tapdata.dummy.constants.RecordOperators;
import io.tapdata.dummy.constants.SyncStage;
import io.tapdata.dummy.po.DummyOffset;
import io.tapdata.dummy.utils.TapEventBuilder;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DelayCalculation;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Dummy connector for testing
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/6/21 18:45 Create
 */
@TapConnectorClass("spec.json")
public class DummyConnector extends ConnectorBase {
    private static final String TAG = DummyConnector.class.getSimpleName();

    private Map<String, TapTable> schemas;
    private Boolean writeLog;
    private IRate writeRate;
    private IDummyConfig config;
    private final TapEventBuilder builder = new TapEventBuilder();
    private DelayCalculation delayCalculation;

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        TapLogger.info(TAG, "Start dummy connector");

        config = IDummyConfig.connectionConfig(connectionContext);
        Integer writeInterval = config.getWriteInterval();
        Integer writeIntervalTotals = config.getWriteIntervalTotals();
        writeRate = IRate.getInstance(writeInterval, writeIntervalTotals);
        writeLog = config.isWriteLog();
        delayCalculation = new DelayCalculation(writeIntervalTotals);

        schemas = new LinkedHashMap<>();
        config.getSchemas().forEach(table -> {
            schemas.put(table.getName(), table);
        });
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        if (delayCalculation.hasData()) {
            TapLogger.info(TAG, "Stop connector: {}", delayCalculation);
        } else {
            TapLogger.info(TAG, "Stop connector");
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        TapLogger.info(TAG, "Register capabilities");

        // support initial sync
        connectorFunctions.supportBatchCount(this::supportBatchCount);
        connectorFunctions.supportBatchRead(this::supportBatchRead);
        // support incremental sync
        connectorFunctions.supportStreamRead(this::supportStreamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::supportTimestampToStreamOffset);
        // target DDL
//        connectorFunctions.supportCreateTable(this::supportCreateTable);
//        connectorFunctions.supportDropTable(this::supportDropTable);
//        connectorFunctions.supportClearTable(this::supportClearTable);
//        connectorFunctions.supportCreateIndex(this::supportCreateIndex);
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        // target DML
        connectorFunctions.supportWriteRecord(this::supportWriteRecord);
        // test and inspect
//        connectorFunctions.supportQueryByAdvanceFilter(this::supportQueryByAdvanceFilter);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        TapLogger.info(TAG, "Discover dummy schema");

        // 过滤表
        List<TapTable> tableSchemas = new ArrayList<>();
        if (null != tables && !tables.isEmpty()) {
            for (String tn : tables) {
                tableSchemas.add(schemas.get(tn));
            }
        } else {
            tableSchemas.addAll(schemas.values());
        }

        consumer.accept(tableSchemas);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        TapLogger.info(TAG, "Connection test");

        onStart(connectionContext);

        consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, null));
        return null;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        TapLogger.info(TAG, "Table count");
        return null == schemas ? 0 : schemas.size();
    }

    private long supportBatchCount(TapConnectorContext nodeContext, TapTable table) throws Throwable {
        return config.getInitialTotals();
    }

    private void supportBatchRead(TapConnectorContext connectorContext, TapTable table, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventConsumer) throws Throwable {
        TapLogger.info(TAG, "start {} batch read", table.getName());

        // Generate specified amount of data
        builder.reset(offsetState, SyncStage.Initial);
        try (
                IBatchConsumer<TapEvent> batchConsumer =
                        IBatchConsumer.getInstance(
                                eventBatchSize,
                                config.getBatchTimeouts(),
                                (t) -> eventConsumer.accept(t, builder.getOffset())
                        )
        ) {
            // generate insert record event
            TapInsertRecordEvent tapInsertRecordEvent;
            Long initialTotals = config.getInitialTotals();
            for (int dataIndex = 0; dataIndex < initialTotals && isAlive(); dataIndex++) {
                tapInsertRecordEvent = builder.generateInsertRecordEvent(table);
                batchConsumer.accept(tapInsertRecordEvent);
            }
        }
        TapLogger.info(TAG, "compile {} batch read", table.getName());
    }

    private void supportStreamRead(TapConnectorContext connectorContext, List<String> tableList, Object offsetState, int eventBatchSize, StreamReadConsumer eventConsumer) throws Throwable {
        TapLogger.info(TAG, "start {} stream read", tableList);

        Integer incrementalInterval = config.getIncrementalInterval();
        Integer incrementalIntervalTotals = config.getIncrementalIntervalTotals();
        Set<RecordOperators> operators = config.getIncrementalTypes();

        TapTable table;
        Map<String, Object> insertAfter;
        TapInsertRecordEvent insertRecordEvent;
        builder.reset(offsetState, SyncStage.Incremental);
        IRate rate = IRate.getInstance(incrementalInterval, incrementalIntervalTotals);
        eventConsumer.streamReadStarted();
        try (IBatchConsumer<TapEvent> batchConsumer = IBatchConsumer.getInstance(eventBatchSize, config.getBatchTimeouts(), (t) -> eventConsumer.accept(t, builder.getOffset()))) {
            while (isAlive()) {
                for (String tableName : tableList) {
                    table = schemas.get(tableName);
                    if (null == table) {
                        throw new RuntimeException(String.format("not found table schema: %s", tableName));
                    }

                    if (operators.contains(RecordOperators.Insert)) {
                        insertRecordEvent = builder.generateInsertRecordEvent(table);
                        insertAfter = new HashMap<>(insertRecordEvent.getAfter());
                        if (!rate.addReturn()) return;
                        batchConsumer.accept(insertRecordEvent);
                    } else {
                        insertAfter = null;
                    }
                    if (operators.contains(RecordOperators.Update)) {
                        if (!rate.addReturn()) return;
                        batchConsumer.accept(builder.generateUpdateRecordEvent(table, insertAfter));
                    }
                    if (operators.contains(RecordOperators.Delete)) {
                        if (!rate.addReturn()) return;
                        batchConsumer.accept(builder.generateDeleteRecordEvent(table, insertAfter));
                    }
                }
            }
        }
        TapLogger.info(TAG, "compile {} batch read", tableList);
    }

    private Object supportTimestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) throws Throwable {
        if (null == offsetStartTime) {
            offsetStartTime = System.currentTimeMillis();
        }

        DummyOffset offset = new DummyOffset();
        offset.setLastTimes(offsetStartTime);
        offset.setBeginTimes(offsetStartTime);
        offset.setLastTN(0);
        return offset;
    }

    private void supportWriteRecord(TapConnectorContext connectorContext, List<TapRecordEvent> recordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        if (null != recordEvents) {
            AtomicLong insert = new AtomicLong();
            AtomicLong update = new AtomicLong();
            AtomicLong delete = new AtomicLong();
            for (TapRecordEvent e : recordEvents) {
                if (!(isAlive() && writeRate.addReturn())) return;

                delayCalculation.log(e.getTime());

                if (e instanceof TapInsertRecordEvent) {
                    insert.addAndGet(1);
                    if (writeLog) {
                        TapLogger.info(TAG, "write insert record: {}", ((TapInsertRecordEvent) e).getAfter());
                    }
                } else if (e instanceof TapUpdateRecordEvent) {
                    update.addAndGet(1);
                    if (writeLog) {
                        TapLogger.info(TAG, "write update record, before: {}, after: {}", ((TapUpdateRecordEvent) e).getBefore(), ((TapUpdateRecordEvent) e).getAfter());
                    }
                } else if (e instanceof TapDeleteRecordEvent) {
                    delete.addAndGet(1);
                    if (writeLog) {
                        TapLogger.info(TAG, "write delete record: {}", ((TapDeleteRecordEvent) e).getBefore());
                    }
                }
            }

            WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
            consumer.accept(listResult
                    .insertedCount(insert.get())
                    .modifiedCount(update.get())
                    .removedCount(delete.get()));
        }
    }

    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) {
        if (writeLog) {
            TapLogger.info(TAG, "Show field DDL: {}", tapFieldBaseEvent.toString());
        }
    }

    private void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
        List<String> batchList = new ArrayList<>();
        for (String table : schemas.keySet()) {
            batchList.add(table);
            if (batchList.size() >= batchSize) {
                listConsumer.accept(batchList);
                batchList = new ArrayList<>();
            }
        }
        if (!batchList.isEmpty()) {
            listConsumer.accept(batchList);
        }
    }
}
