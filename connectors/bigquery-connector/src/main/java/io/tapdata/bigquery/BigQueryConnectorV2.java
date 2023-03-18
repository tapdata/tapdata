package io.tapdata.bigquery;

import com.google.protobuf.Descriptors;
import io.tapdata.base.ConnectorBase;
import io.tapdata.bigquery.entity.ContextConfig;
import io.tapdata.bigquery.service.bigQuery.BigQueryConnectionTest;
import io.tapdata.bigquery.service.bigQuery.TableCreate;
import io.tapdata.bigquery.service.bigQuery.WriteRecord;
import io.tapdata.bigquery.service.command.Command;
import io.tapdata.bigquery.service.stream.v2.BigQueryStream;
import io.tapdata.bigquery.service.stream.v2.MergeHandel;
import io.tapdata.bigquery.service.stream.v2.StateMapOperator;
import io.tapdata.bigquery.service.stream.v2.WriteBigQueryException;
import io.tapdata.bigquery.util.bigQueryUtil.FieldChecker;
import io.tapdata.bigquery.util.bigQueryUtil.SqlValueConvert;
import io.tapdata.bigquery.util.tool.Checker;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapdata.write.WriteValve;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@TapConnectorClass("spec-v2.json")
public class BigQueryConnectorV2 extends ConnectorBase {
    private static final String TAG = BigQueryConnector.class.getSimpleName();
    private static final int STREAM_SIZE = 30000;
    private static final int CUMULATIVE_TIME_INTERVAL = 1;

    private WriteRecord writeRecord;
    private TableCreate tableCreate;
    private BigQueryStream stream;
    private MergeHandel merge;
    private final AtomicBoolean running = new AtomicBoolean(Boolean.TRUE);
    private String tableId;
    private WriteValve valve;
    private final Object lock = new Object();
    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        this.writeRecord = (WriteRecord) WriteRecord.create(connectionContext).autoStart();
        this.tableCreate = (TableCreate) TableCreate.create(connectionContext).paperStart(this.writeRecord);
        if (connectionContext instanceof TapConnectorContext) {
            TapConnectorContext context = (TapConnectorContext) connectionContext;
            isConnectorStarted(connectionContext, connectorContext -> {
                Iterator<Entry<TapTable>> iterator = connectorContext.getTableMap().iterator();
                while (iterator.hasNext()) {
                    Entry<TapTable> next = iterator.next();
                    TapTable value = next.getValue();
                    if (Checker.isNotEmpty(value)) {
                        FieldChecker.verifyFieldName(value.getNameFieldMap());
                    }
                }
            });
            ContextConfig config = this.writeRecord.config();
            this.stream = (BigQueryStream) BigQueryStream.streamWrite(context).paperStart(this.writeRecord);
            this.merge = ((MergeHandel) MergeHandel.merge(connectionContext).paperStart(writeRecord))
                    .running(this.running)
                    .mergeDelaySeconds(config.mergeDelay());
            this.stream.merge(this.merge);
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        synchronized (this) {
            this.notify();
        }
        this.running.set(false);
        Optional.ofNullable(this.writeRecord).ifPresent(WriteRecord::onDestroy);
        try {
            Optional.ofNullable(this.merge).ifPresent(MergeHandel::stop);
            Optional.ofNullable(this.stream).ifPresent(BigQueryStream::closeStream);
        }finally {
            Optional.ofNullable(this.valve).ifPresent(WriteValve::close);
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        //codecRegistry.registerFromTapValue(TapYearValue.class, "DATE", TapValue::getValue);
        codecRegistry.registerFromTapValue(TapYearValue.class, "INT64", TapValue::getValue);
        codecRegistry.registerFromTapValue(TapMapValue.class, "JSON", tapValue -> toJson(tapValue.getValue()));
        codecRegistry.registerFromTapValue(TapArrayValue.class, "JSON", tapValue -> toJson(tapValue.getValue()));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, "TIMESTAMP",tapValue -> formatTapDateTime(tapValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS"));//2023-01-12 09:57:27.628000 UTC
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, "DATETIME",tapValue -> FieldChecker.simpleDateValue(tapValue.getValue(), "yyyy-MM-dd HH:mm:ss",false).replace(" ","T"));//2023-01-12 09:57:27.628000 UTC
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTimeStr());
        codecRegistry.registerFromTapValue(TapYearValue.class, tapYearValue -> formatTapDateTime(tapYearValue.getValue(), "yyyy"));

        connectorFunctions.supportWriteRecord(this::writeRecord)
                .supportCommandCallbackFunction(this::command)
                .supportCreateTableV2(this::createTableV2)
                .supportClearTable(this::clearTable)
                .supportDropTable(this::dropTable)
                .supportReleaseExternalFunction(this::release)
                .supportErrorHandleFunction(this::errorHandle)
        ;
    }

    private synchronized void release(TapConnectorContext context) {
        KVMap<Object> stateMap = context.getStateMap();
        Object temporaryConfig = stateMap.get(StateMapOperator.TABLE_CONFIG_NAME);//(ContextConfig.TEMP_CURSOR_SCHEMA_NAME);
        if (Objects.nonNull(temporaryConfig)) {
            Map<String,Object> config = (Map<String, Object>)temporaryConfig;
            List<String > temporaryIds = new ArrayList<>();
            for (Map.Entry<String, Object> entry : config.entrySet()) {
                Optional.ofNullable(entry).flatMap(ent -> Optional.ofNullable(ent.getValue())).ifPresent(t -> {
                    Map<String, Object> entryConfig = (Map<String, Object>) t;
                    Optional.ofNullable(entryConfig.get(ContextConfig.TEMP_CURSOR_SCHEMA_NAME))
                            .ifPresent(schema -> temporaryIds.add(String.valueOf(schema)));
                });
            }
            this.merge = (MergeHandel) MergeHandel.merge(context).autoStart();
            //删除临时表
            try {
                this.merge.dropTemporaryTable(temporaryIds);
                this.merge.config().tempCursorSchema(null);
                stateMap.put(StateMapOperator.TABLE_CONFIG_NAME, null);
            } catch (Exception e) {
                TapLogger.warn(TAG, " Temporary table cannot be drop temporarily. Details: " + e.getMessage());
            }
        }
    }

    private void dropTable(TapConnectorContext context, TapDropTableEvent dropTableEvent) {
        if (Objects.isNull(dropTableEvent)){
            throw new CoreException("TapDropTableEvent cannot be empty.");
        }
        this.tableCreate.dropTable(dropTableEvent);
        if (Objects.nonNull(this.merge)) {
            try {
                this.merge.dropTemporaryTableByMainTable(dropTableEvent.getTableId());
            } catch (Exception e) {
                TapLogger.info(TAG, " Temporary table data cannot be cleared temporarily. Details: " + e.getMessage());
            }
        }
    }

    private void clearTable(TapConnectorContext connectorContext, TapClearTableEvent clearTableEvent) {
        try {
            this.tableCreate.cleanTable(clearTableEvent);
            String tableId = clearTableEvent.getTableId();
            if (Objects.nonNull(this.merge)) {
                try {
                    this.merge.cleanTemporaryTable(tableId);
                } catch (Exception e) {
                    TapLogger.warn(TAG, " Temporary table data cannot be cleared temporarily. Details: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            TapLogger.warn(TAG, " Table data cannot be cleared temporarily. Details: " + e.getMessage());
        }
    }

    private CreateTableOptions createTableV2(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) {
        CreateTableOptions createTableOptions = CreateTableOptions.create().tableExists(tableCreate.isExist(createTableEvent));
        if (!createTableOptions.getTableExists()) {
            this.tableCreate.createSchema(createTableEvent);
        }
        return createTableOptions;
    }

    private CommandResult command(TapConnectionContext context, CommandInfo commandInfo) {
        return Command.command(context, commandInfo);
    }

    private void writeRecord(TapConnectorContext context, List<TapRecordEvent> events, TapTable table, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        this.tableId = table.getId();
        this.stream.tapTable(table);
        this.writeRecordStream(context, events, table, consumer);
    }

    private void writeRecordStream(TapConnectorContext context, List<TapRecordEvent> events, TapTable table, Consumer<WriteListResult<TapRecordEvent>> consumer) {
        synchronized (this.lock) {
            if (Objects.isNull(this.valve)) {
                this.valve = WriteValve.open(
                        BigQueryConnectorV2.STREAM_SIZE,
                        BigQueryConnectorV2.CUMULATIVE_TIME_INTERVAL,
                        (writeConsumer, writeList, targetTable) -> {
                            try {
                                writeConsumer.accept(this.stream.writeRecord(writeList, targetTable));
                            } catch (Exception e) {
                                if (e instanceof WriteBigQueryException){
                                    throw new CoreException(e.getMessage());
                                }else {
                                    TapLogger.error(TAG, "uploadEvents size {} to table {} failed, {}", writeList.size(), targetTable.getId(), e.getMessage());
                                }
                            }
                        },
                        (writeList, targetTable) -> {
                            LinkedHashMap<String, TapField> nameFieldMap = targetTable.getNameFieldMap();
                            if (Objects.isNull(nameFieldMap) || nameFieldMap.isEmpty()) {
                                throw new CoreException("TapTable not any fields.");
                            }
                            for (TapRecordEvent event : writeList) {
                                if (Objects.isNull(event)) continue;
                                Map<String, Object> record = new HashMap<>();
                                if (event instanceof TapInsertRecordEvent) {
                                    Map<String, Object> recordMap = new HashMap<>();
                                    TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) event;
                                    record = insertRecordEvent.getAfter();
                                    Map<String, Object> finalRecord = record;
                                    nameFieldMap.forEach((key, f) -> {
                                        Object value = finalRecord.get(key);
                                        if (Objects.nonNull(value)) {
                                            recordMap.put(key, SqlValueConvert.streamJsonArrayValue(value, f));
                                        }
                                    });
                                    insertRecordEvent.after(recordMap);
                                }
                            }
                        },
                        consumer
                ).initDelay(1).start();
            }
        }
        this.valve.write(events,table);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        this.tableCreate.discoverSchema(tables, tableSize, consumer);
    }


    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        BigQueryConnectionTest bigQueryConnectionTest = (BigQueryConnectionTest) BigQueryConnectionTest.create(connectionContext).autoStart();
        TestItem testItem = bigQueryConnectionTest.testServiceAccount();
        consumer.accept(testItem);
        if (TestItem.RESULT_FAILED == testItem.getResult()) {
            return connectionOptions;
        }
        TestItem tableSetItem = bigQueryConnectionTest.testTableSet();
        consumer.accept(tableSetItem);
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return ((TableCreate) TableCreate.create(connectionContext).autoStart()).schemaCount();
    }
}
