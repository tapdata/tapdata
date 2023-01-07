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
	private static final int STREAM_SIZE = 60000;
	private static final String STREAM_OFFSET_KEY_NAME = "STREAM_API_OFFSET";

	private WriteRecord writeRecord;
	private TapEventCollector tapEventCollector;
	private BigQueryStream stream;
	private MergeHandel merge ;
	private ScheduledFuture<?> future;
	private final AtomicBoolean running = new AtomicBoolean(Boolean.TRUE);
    private final AtomicLong streamOffset = new AtomicLong(0);
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
			ContextConfig config =this.writeRecord.config();
			this.stream = BigQueryStream.streamWrite(context);
			if (Objects.nonNull(config) && config.isMixedUpdates()) {
				this.merge = MergeHandel.merge(connectionContext)
						.running(this.running)
						.mergeDelaySeconds(config.mergeDelay())
						.temporaryTableId(config.tempCursorSchema());
				this.stream.merge(this.merge);
			}
		}
	}
	private void saveOffsetToStateMap(TapConnectorContext context){
		KVMap<Object> stateMap = context.getStateMap();
		if (Objects.isNull(stateMap)){
			throw new CoreException("Task's state map can not be null or not be empty.");
		}
		stateMap.put(BigQueryConnector.STREAM_OFFSET_KEY_NAME,this.streamOffset.get());
	}
	private long getOffsetFromStateMap(TapConnectorContext context){
		KVMap<Object> stateMap = context.getStateMap();
		if (Objects.isNull(stateMap)){
			throw new CoreException("Task's state map can not be null or not be empty.");
		}
		try {
			return (Long) stateMap.get(BigQueryConnector.STREAM_OFFSET_KEY_NAME);
		}catch (Exception e){
			return 0L;
		}
	}

	@Override
	public void onStop(TapConnectionContext connectionContext) throws Throwable {
		synchronized (this) {
			this.notify();
		}
		Optional.ofNullable(this.writeRecord).ifPresent(WriteRecord::onDestroy);
		Optional.ofNullable(this.tapEventCollector).ifPresent(TapEventCollector::stop);
		this.running.set(false);
		Optional.ofNullable(this.merge).ifPresent(MergeHandel::stop);
		Optional.ofNullable(this.future).ifPresent(consumer->{
			consumer.cancel(true);
			if (connectionContext instanceof TapConnectorContext){
				this.saveOffsetToStateMap((TapConnectorContext) connectionContext);
			}
		});
		Optional.ofNullable(this.stream).ifPresent(BigQueryStream::closeStream);
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
		if( Objects.nonNull(this.merge) && this.merge.config().isMixedUpdates() ) {
			KVMap<Object> stateMap = context.getStateMap();
			Object tempCursorSchema = stateMap.get(ContextConfig.TEMP_CURSOR_SCHEMA_NAME);
			if(Objects.isNull(tempCursorSchema)){
				TapLogger.info(TAG,"Cache Schema has created ,named is " + tempCursorSchema);
			}
			this.merge.dropTemporaryTable(String.valueOf(tempCursorSchema));
		}
    }

    private void clearTable(TapConnectorContext connectorContext, TapClearTableEvent clearTableEvent) {
		TableCreate tableCreate = TableCreate.create(connectorContext);
		tableCreate.cleanTable(clearTableEvent);
		if(Objects.nonNull(this.merge) && this.merge.config().isMixedUpdates()) {
			this.merge.cleanTemporaryTable();
		}
    }

    private CreateTableOptions createTableV2(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) {
		TableCreate tableCreate = TableCreate.create(connectorContext);
		CreateTableOptions createTableOptions = CreateTableOptions.create().tableExists(tableCreate.isExist(createTableEvent));
		if (!createTableOptions.getTableExists()){
			tableCreate.createSchema(createTableEvent);
		}
		if(Objects.nonNull(this.merge) && this.merge.config().isMixedUpdates()) {
			this.merge.createTemporaryTable(createTableEvent.getTable(),tableCreate.config().tempCursorSchema());
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
		//定期更新stream API 每批提交的数据绑定的offset 到 stateMap
		this.streamOffset.set(this.getOffsetFromStateMap(context));
		if ( Objects.isNull(this.future)){
			this.future = this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
				try {
					this.saveOffsetToStateMap(context);
				} catch (Throwable throwable) {
					TapLogger.error(TAG, "Try upload failed in scheduler, {}", throwable.getMessage());
				}
			}, 10, 300, TimeUnit.SECONDS);
		}
		this.stream.streamOffset(this.streamOffset).tapTable(table);
		if (Objects.isNull(this.stream.writeCommittedStream())) {
            this.stream.createWriteCommittedStream();
        }
        this.writeRecordStream(context, events, table, consumer);
        if (Objects.nonNull(this.merge) && this.merge.config().isMixedUpdates()){
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
		if (Objects.isNull(this.tapEventCollector)) {
			synchronized (this) {
				if (Objects.isNull(this.tapEventCollector)) {
					this.tapEventCollector = TapEventCollector.create()
							.maxRecords(BigQueryConnector.STREAM_SIZE)
							.idleSeconds(5)
							.table(table)
							.writeListResultConsumer(consumer)
							.eventCollected(this::uploadEvents);
					this.tapEventCollector.start();
				}
			}
		}
		this.tapEventCollector.addTapEvents(events,table,this.writeRecord.config().isMixedUpdates());
	}

	/**
	 * @deprecated
	 * */
	private void writeRecordDML(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer){
		if (Objects.isNull(this.writeRecord)){
			this.writeRecord = WriteRecord.create(connectorContext);
		}
		this.writeRecord.writeBatch(tapRecordEvents, tapTable, writeListResultConsumer);
	}


	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
		TableCreate tableCreate = TableCreate.create(connectionContext);
		tableCreate.discoverSchema(tables, tableSize, consumer);
	}


	@Override
	public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
		ConnectionOptions connectionOptions = ConnectionOptions.create();
		BigQueryConnectionTest bigQueryConnectionTest = BigQueryConnectionTest.create(connectionContext);
		TestItem testItem = bigQueryConnectionTest.testServiceAccount();
		consumer.accept(testItem);
		if ( TestItem.RESULT_FAILED == testItem.getResult()){
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
