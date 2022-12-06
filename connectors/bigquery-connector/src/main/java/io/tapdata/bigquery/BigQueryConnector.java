package io.tapdata.bigquery;

import io.tapdata.base.ConnectorBase;
import io.tapdata.bigquery.service.bigQuery.BigQueryConnectionTest;
import io.tapdata.bigquery.service.bigQuery.TableCreate;
import io.tapdata.bigquery.service.bigQuery.WriteRecord;
import io.tapdata.bigquery.service.command.Command;
import io.tapdata.bigquery.service.stage.tapvalue.ValueHandel;
import io.tapdata.bigquery.util.bigQueryUtil.FieldChecker;
import io.tapdata.bigquery.util.tool.Checker;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
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
	private WriteRecord writeRecord;
	private ValueHandel valueHandel;//= ValueHandel.create();

	@Override
	public void onStart(TapConnectionContext connectionContext) throws Throwable {
        this.writeRecord = WriteRecord.create(connectionContext);
		if (connectionContext instanceof TapConnectorContext) {
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

    private void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) {
		TableCreate tableCreate = TableCreate.create(connectorContext);
		tableCreate.dropTable(dropTableEvent);
    }

    private void clearTable(TapConnectorContext connectorContext, TapClearTableEvent clearTableEvent) {
		TableCreate tableCreate = TableCreate.create(connectorContext);
		tableCreate.cleanTable(clearTableEvent);
    }

    private CreateTableOptions createTableV2(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) {
		TableCreate tableCreate = TableCreate.create(connectorContext);
		CreateTableOptions createTableOptions = CreateTableOptions.create().tableExists(tableCreate.isExist(createTableEvent));
		if (!createTableOptions.getTableExists()){
			tableCreate.createSchema(createTableEvent);
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

	private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
		if (null == this.writeRecord){
//            this.valueHandel = new ValueHandel();
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
