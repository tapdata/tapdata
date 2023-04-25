package io.tapdata.bigquery;

import com.google.protobuf.Descriptors;
import io.tapdata.base.ConnectorBase;
import io.tapdata.bigquery.entity.ContextConfig;
import io.tapdata.bigquery.service.bigQuery.BigQueryConnectionTest;
import io.tapdata.bigquery.service.bigQuery.TableCreate;
import io.tapdata.bigquery.service.bigQuery.WriteRecord;
import io.tapdata.bigquery.service.command.Command;
import io.tapdata.bigquery.service.stream.handle.BigQueryStream;
import io.tapdata.bigquery.service.stream.handle.MergeHandel;
import io.tapdata.bigquery.util.bigQueryUtil.FieldChecker;
import io.tapdata.bigquery.util.bigQueryUtil.SqlValueConvert;
import io.tapdata.bigquery.util.tool.Checker;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.schema.value.TapYearValue;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVMap;
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

//@TapConnectorClass("spec.json")
public class BigQueryConnector extends ConnectorBase {
    private static final String TAG = BigQueryConnector.class.getSimpleName();
    private static final int STREAM_SIZE = 30000;
    private static final int CUMULATIVE_TIME_INTERVAL = 5;
    private WriteRecord writeRecord;
    private TableCreate tableCreate;
    private BigQueryStream stream;
    private MergeHandel merge;
    private final AtomicBoolean running = new AtomicBoolean(Boolean.TRUE);
    private String tableId;
    //private WriteValve valve;

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
            if (Objects.nonNull(config) && config.isMixedUpdates()) {
                this.merge = ((MergeHandel) MergeHandel.merge(connectionContext).paperStart(writeRecord))
                        .running(this.running)
                        .mergeDelaySeconds(config.mergeDelay());
                this.stream.merge(this.merge);
            }
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        synchronized (this) {
            this.notify();
        }
        Optional.ofNullable(this.writeRecord).ifPresent(WriteRecord::onDestroy);
        //Optional.ofNullable(this.valve).ifPresent(WriteValve::close);
        this.running.set(false);
        Optional.ofNullable(this.merge).ifPresent(MergeHandel::stop);
        //Optional.ofNullable(this.stream).ifPresent(BigQueryStream::closeStream);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        //codecRegistry.registerFromTapValue(TapYearValue.class, "DATE", TapValue::getValue);
        codecRegistry.registerFromTapValue(TapYearValue.class, "INT64", TapValue::getValue);
        codecRegistry.registerFromTapValue(TapMapValue.class, "JSON", tapValue -> toJson(tapValue.getValue()));
        codecRegistry.registerFromTapValue(TapArrayValue.class, "JSON", tapValue -> toJson(tapValue.getValue()));
        connectorFunctions.supportWriteRecord(this::writeRecord)
                .supportCommandCallbackFunction(this::command)
                .supportCreateTableV2(this::createTableV2)
                .supportClearTable(this::clearTable)
                .supportDropTable(this::dropTable)
                .supportReleaseExternalFunction(this::release)
                .supportErrorHandleFunction(this::errorHandle)
        ;
    }

    private void release(TapConnectorContext context) {
        KVMap<Object> stateMap = context.getStateMap();
        Object temporaryTable = stateMap.get(ContextConfig.TEMP_CURSOR_SCHEMA_NAME);
        if (Objects.nonNull(temporaryTable)) {
            merge = (MergeHandel) MergeHandel.merge(context).autoStart();
            //清除临时表
            if (Objects.nonNull(merge.config()) && Objects.nonNull(this.merge.config().tempCursorSchema())) {
                if (Objects.nonNull(tableId) && !"".equals(tableId.trim())) {
                    try {
                        merge.mainTable(table(tableId)).mergeTableOnce();
                    } catch (Exception e) {
                        throw new CoreException(String.format(" The temporary table is removed during the reset operation. Moving out and merging the remaining data failed. Temporary table name:%s, target table name:%s. ", tableId, this.merge.config().tempCursorSchema()));
                    }
                }
                this.merge.dropTemporaryTable();
                this.merge.config().tempCursorSchema(null);
                stateMap.put(ContextConfig.TEMP_CURSOR_SCHEMA_NAME, null);
            }
        }
    }

    private void dropTable(TapConnectorContext context, TapDropTableEvent dropTableEvent) {
        this.tableCreate.dropTable(dropTableEvent);
    }

    private void clearTable(TapConnectorContext connectorContext, TapClearTableEvent clearTableEvent) {
        this.tableCreate.cleanTable(clearTableEvent);
        if (Objects.nonNull(this.merge) && this.merge.config().isMixedUpdates()) {
            this.merge.cleanTemporaryTable();
        }
    }

    private CreateTableOptions createTableV2(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) {
        CreateTableOptions createTableOptions = CreateTableOptions.create().tableExists(tableCreate.isExist(createTableEvent));
        if (!createTableOptions.getTableExists()) {
            this.tableCreate.createSchema(createTableEvent);
        }
        if (Objects.nonNull(this.merge) && this.merge.config().isMixedUpdates()) {
            this.merge.createTemporaryTable(createTableEvent.getTable(), this.tableCreate.config().tempCursorSchema());
        }
        return createTableOptions;
    }

    private CommandResult command(TapConnectionContext context, CommandInfo commandInfo) {
        return Command.command(context, commandInfo);
    }

    private void createTable(TapConnectorContext connectorContext, TapCreateTableEvent tapCreateTableEvent) {
        if (!this.tableCreate.isExist(tapCreateTableEvent)) {
            this.tableCreate.createSchema(tapCreateTableEvent);
        }
    }

    private void writeRecord(TapConnectorContext context, List<TapRecordEvent> events, TapTable table, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        this.tableId = table.getId();
        this.stream.tapTable(table);
        this.writeRecordStream(context, events, table, consumer);
        if (Objects.nonNull(this.merge) && this.merge.config().isMixedUpdates()) {
            this.merge.mergeTemporaryTableToMainTable(table);
        }
    }

    private void uploadEvents(Consumer<WriteListResult<TapRecordEvent>> consumer, List<TapRecordEvent> events, TapTable table) {
        try {
            consumer.accept(this.stream.writeRecord(events, table));
        } catch (Exception e) {
            TapLogger.error(TAG, e.getMessage());
        }
    }

    private void writeRecordStream(TapConnectorContext context, List<TapRecordEvent> events, TapTable table, Consumer<WriteListResult<TapRecordEvent>> consumer) {
        //if (Objects.isNull(this.valve)) {
        //    this.valve = WriteValve.open(
        //            BigQueryConnector.STREAM_SIZE,
        //            BigQueryConnector.CUMULATIVE_TIME_INTERVAL,
        //            (writeConsumer, writeList, targetTable) -> {
        //                try {
        //                    writeConsumer.accept(this.stream.writeRecord(writeList, targetTable));
        //                } catch (Exception e) {
        //                    TapLogger.warn(TAG, "uploadEvents size {} to table {} failed, {}", writeList.size(), targetTable.getId(), e.getMessage());
        //                }
        //            },
        //            consumer
        //    ).write(events,table);
        //}

        try {
            consumer.accept(this.stream.writeRecord(events, table));
        } catch (Exception e) {
            TapLogger.warn(TAG, "uploadEvents size {} to table {} failed, {}", events.size(), table.getId(), e.getMessage());
        }
    }

    /**
     * @deprecated Because QPS is too low .
     */
    private void writeRecordDML(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        if (Objects.isNull(this.writeRecord)) {
            this.writeRecord = WriteRecord.create(connectorContext);
        }
        this.writeRecord.writeBatch(tapRecordEvents, tapTable, writeListResultConsumer);
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
