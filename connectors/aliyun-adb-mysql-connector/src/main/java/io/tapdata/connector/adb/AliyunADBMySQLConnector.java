package io.tapdata.connector.adb;

import io.tapdata.connector.adb.write.AliyunADBBatchWriter;
import io.tapdata.connector.mysql.MysqlConnector;
import io.tapdata.connector.mysql.writer.MysqlSqlBatchWriter;
import io.tapdata.connector.mysql.writer.MysqlWriter;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author jackin
 * @Description
 * @create 2022-12-12 15:18
 **/
@TapConnectorClass("aliyun-adb-mysql-spec.json")
public class AliyunADBMySQLConnector extends MysqlConnector {
	private MysqlWriter mysqlWriter;
	private MysqlJdbcContext aliyunADBJdbcContext;

	@Override
	public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
		tapConnectionContext.getConnectionConfig().put("protocolType", "mysql");
		super.onStart(tapConnectionContext);
		this.aliyunADBJdbcContext = new MysqlJdbcContext(tapConnectionContext);
		if (tapConnectionContext instanceof TapConnectorContext) {
			this.mysqlWriter = new AliyunADBBatchWriter(aliyunADBJdbcContext);
		}
	}

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
		super.registerCapabilities(connectorFunctions, codecRegistry);
		connectorFunctions.supportWriteRecord(this::writeRecord);
	}

	@Override
	public ConnectionOptions connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) {
		databaseContext.getConnectionConfig().put("protocolType", "mysql");
		return super.connectionTest(databaseContext, consumer);
	}

	private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
		WriteListResult<TapRecordEvent> writeListResult = this.mysqlWriter.write(tapConnectorContext, tapTable, tapRecordEvents);
		consumer.accept(writeListResult);
	}
}
