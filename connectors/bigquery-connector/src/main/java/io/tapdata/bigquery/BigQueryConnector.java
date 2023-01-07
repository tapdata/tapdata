package io.tapdata.bigquery;

import com.google.protobuf.Descriptors;
import io.tapdata.base.ConnectorBase;
import io.tapdata.bigquery.entity.ContextConfig;
import io.tapdata.bigquery.service.bigQuery.BigQueryConnectionTest;
import io.tapdata.bigquery.service.bigQuery.TableCreate;
import io.tapdata.bigquery.service.bigQuery.WriteRecord;
import io.tapdata.bigquery.service.command.Command;
import io.tapdata.bigquery.service.stage.tapvalue.ValueHandel;
import io.tapdata.bigquery.service.stream.handle.BigQueryStream;
import io.tapdata.bigquery.service.stream.handle.MergeHandel;
import io.tapdata.bigquery.service.stream.handle.TapEventCollector;
import io.tapdata.bigquery.util.bigQueryUtil.FieldChecker;
import io.tapdata.bigquery.util.tool.Checker;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.schema.value.TapYearValue;
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

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class BigQueryConnector extends ConnectorBase {
	private static final String TAG = BigQueryConnector.class.getSimpleName();

	private final Object streamReadLock = new Object();
	private WriteRecord writeRecord;
	private ValueHandel valueHandel;//= ValueHandel.create();
	private TapEventCollector tapEventCollector;
	BigQueryStream stream;
	public static final int STREAM_SIZE = 20000;
	MergeHandel merge ;
	AtomicBoolean running = new AtomicBoolean(true);


    private final AtomicLong streamOffset = new AtomicLong(0);
    public static final String STREAM_OFFSET_KEY_NAME = "STREAM_API_OFFSET";

	private ScheduledFuture<?> future;
	private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

	@Override
	public void onStart(TapConnectionContext connectionContext) throws Throwable {
        this.writeRecord = WriteRecord.create(connectionContext);
		if (connectionContext instanceof TapConnectorContext) {
			TapConnectorContext context = (TapConnectorContext)connectionContext;

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
			ContextConfig config = writeRecord.config();
			merge = MergeHandel.merge(connectionContext).running(running);
            if (Objects.nonNull(config)){
                merge.mergeDelaySeconds(config.mergeDelay())
                        .temporaryTableId(config.tempCursorSchema());
            }
            stream = BigQueryStream.streamWrite(context)
                    .merge(merge)
                    .streamOffset(streamOffset);

            //定期更新stream API 每批提交的数据绑定的offset 到 stateMap
			if (writeRecord.config().isMixedUpdates()) {
				streamOffset.set(this.getOffsetFromStateMap(context));
				if ( Objects.isNull(future)){
					future = scheduledExecutorService.scheduleWithFixedDelay(() -> {
						try {
							this.saveOffsetToStateMap(context);
						} catch (Throwable throwable) {
							TapLogger.error(TAG, "Try upload failed in scheduler, {}", throwable.getMessage());
						}
					}, 10, 600, TimeUnit.SECONDS);
				}
			}
		}
	}
	private void saveOffsetToStateMap(TapConnectorContext context){
		KVMap<Object> stateMap = context.getStateMap();
		if (Objects.isNull(stateMap)){
			throw new CoreException("Task's state map can not be null or not be empty.");
		}
		stateMap.put(STREAM_OFFSET_KEY_NAME,streamOffset.get());
	}
	private long getOffsetFromStateMap(TapConnectorContext context){
		KVMap<Object> stateMap = context.getStateMap();
		if (Objects.isNull(stateMap)){
			throw new CoreException("Task's state map can not be null or not be empty.");
		}
		try {
			return (Long) stateMap.get(STREAM_OFFSET_KEY_NAME);
		}catch (Exception e){
			return 0L;
		}
	}

	@Override
	public void onStop(TapConnectionContext connectionContext) throws Throwable {
		synchronized (this) {
			this.notify();
		}
		try {
			Optional.ofNullable(this.writeRecord).ifPresent(WriteRecord::onDestroy);
		} catch (Exception ignored) {
		}

		if(Objects.nonNull(tapEventCollector)) {
			tapEventCollector.stop();
		}
		running.set(false);
		if (Objects.nonNull(merge)) {
			merge.stop();
		}
		if (Objects.nonNull(future)) {
			future.cancel(true);
			if (connectionContext instanceof TapConnectorContext){
				this.saveOffsetToStateMap((TapConnectorContext) connectionContext);
			}
		}
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
		;
	}

    private void dropTable(TapConnectorContext context, TapDropTableEvent dropTableEvent) {
		TableCreate tableCreate = TableCreate.create(context);
		tableCreate.dropTable(dropTableEvent);
		if( Objects.nonNull(merge) && merge.config().isMixedUpdates() ) {
			KVMap<Object> stateMap = context.getStateMap();
			Object tempCursorSchema = stateMap.get(ContextConfig.TEMP_CURSOR_SCHEMA_NAME);
			if(Objects.isNull(tempCursorSchema)){
				TapLogger.info(TAG,"Cache Schema has created ,named is "+tempCursorSchema);
			}
			merge.dropTemporaryTable(String.valueOf(tempCursorSchema));
		}
    }

    private void clearTable(TapConnectorContext connectorContext, TapClearTableEvent clearTableEvent) {
		TableCreate tableCreate = TableCreate.create(connectorContext);
		tableCreate.cleanTable(clearTableEvent);
		if(Objects.nonNull(merge)&&merge.config().isMixedUpdates()) {
			merge.cleanTemporaryTable();
		}
    }

    private CreateTableOptions createTableV2(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) {
		TableCreate tableCreate = TableCreate.create(connectorContext);
		CreateTableOptions createTableOptions = CreateTableOptions.create().tableExists(tableCreate.isExist(createTableEvent));
		if (!createTableOptions.getTableExists()){
			tableCreate.createSchema(createTableEvent);
		}
		if(Objects.nonNull(merge) && merge.config().isMixedUpdates()) {
			merge.createTemporaryTable(createTableEvent.getTable(),tableCreate.config().tempCursorSchema());
		}
		return createTableOptions;
    }

    private CommandResult command(TapConnectionContext context, CommandInfo commandInfo) {
		return Command.command(context,commandInfo);
	}

	private void createTable(TapConnectorContext connectorContext, TapCreateTableEvent tapCreateTableEvent) {
		TableCreate tableCreate = TableCreate.create(connectorContext);
		if (!tableCreate.isExist(tapCreateTableEvent)){
			tableCreate.createSchema(tapCreateTableEvent);
		}
	}

	private void writeRecord(TapConnectorContext context, List<TapRecordEvent> events, TapTable table, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        if (Objects.isNull(stream.writeCommittedStream())) {
            this.stream.createWriteCommittedStream();
        }
        this.writeRecordStream(context, events, table, consumer);
        if (writeRecord.config().isMixedUpdates()){
            merge.mergeTemporaryTableToMainTable(table);
        }
	}
	private void uploadEvents(Consumer<WriteListResult<TapRecordEvent>> consumer, List<TapRecordEvent> events, TapTable table) {
		try {
			consumer.accept(stream.writeRecord(events, table));
		} catch (Exception e) {
			TapLogger.error(TAG, e.getMessage());
		}
	}
	private void writeRecordStream(TapConnectorContext context, List<TapRecordEvent> events, TapTable table, Consumer<WriteListResult<TapRecordEvent>> consumer) {
		if (tapEventCollector == null) {
			synchronized (this) {
				if (tapEventCollector == null) {
					tapEventCollector = TapEventCollector.create()
							.maxRecords(STREAM_SIZE)
							.idleSeconds(10)
							.table(table)
							.writeListResultConsumer(consumer)
							.eventCollected(this::uploadEvents);
					tapEventCollector.start();
				}
			}
		}
		tapEventCollector.addTapEvents(events,table,writeRecord.config().isMixedUpdates());
	}

	/**
	 * @deprecated
	 * */
	private void writeRecordDML(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer){
		if (null == this.writeRecord){
			this.writeRecord = WriteRecord.create(connectorContext);
		}
		this.writeRecord.writeBatch(tapRecordEvents, tapTable, writeListResultConsumer);
	}


	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
		TableCreate tableCreate = TableCreate.create(connectionContext);
		tableCreate.discoverSchema(tables,tableSize,consumer);
	}


	@Override
	public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
		ConnectionOptions connectionOptions = ConnectionOptions.create();
		BigQueryConnectionTest bigQueryConnectionTest = BigQueryConnectionTest.create(connectionContext);
		TestItem testItem = bigQueryConnectionTest.testServiceAccount();
		consumer.accept(testItem);
		if ( testItem.getResult() == TestItem.RESULT_FAILED){
			return connectionOptions;
		}
		TestItem tableSetItem = bigQueryConnectionTest.testTableSet();
		consumer.accept(tableSetItem);
		return connectionOptions;
	}

	@Override
	public int tableCount(TapConnectionContext connectionContext) throws Throwable {
		return TableCreate.create(connectionContext).schemaCount();
	}
}
