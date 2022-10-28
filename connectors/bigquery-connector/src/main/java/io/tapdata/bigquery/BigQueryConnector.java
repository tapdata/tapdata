package io.tapdata.bigquery;


import cn.hutool.core.date.DateUtil;
import io.tapdata.base.ConnectorBase;
import io.tapdata.bigquery.enums.BigQueryTestItem;
import io.tapdata.bigquery.service.OpenApiWriteRecoder;
import io.tapdata.bigquery.service.bigQuery.BigQueryConnectionTest;
import io.tapdata.bigquery.service.bigQuery.WriteRecord;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;


import java.util.*;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class BigQueryConnector extends ConnectorBase {
	private static final String TAG = BigQueryConnector.class.getSimpleName();

	private final Object streamReadLock = new Object();
	WriteRecord writeRecord;

	@Override
	public void onStart(TapConnectionContext connectionContext) throws Throwable {
		this.writeRecord = WriteRecord.create(connectionContext);
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
	}

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
//		codecRegistry.registerFromTapValue(TapMapValue.class, "json")
		connectorFunctions.supportWriteRecord(this::writeRecord)
//				.supportCreateTable(this::createTable)

		;
	}

	private void createTable(TapConnectorContext connectorContext, TapCreateTableEvent tapCreateTableEvent) {
		OpenApiWriteRecoder recoder = new OpenApiWriteRecoder();
		if (!recoder.tableExist(tapCreateTableEvent.getTableId())){

		}
	}

	private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
		this.writeRecord.write(tapRecordEvents, tapTable, writeListResultConsumer);
	}


	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {

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
		return 1;
	}
}
