package io.tapdata.connector.adb;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.adb.write.AliyunADBBatchWriter;
import io.tapdata.connector.mysql.MysqlConnectionTest;
import io.tapdata.connector.mysql.MysqlConnector;
import io.tapdata.connector.mysql.writer.MysqlSqlBatchWriter;
import io.tapdata.connector.mysql.writer.MysqlWriter;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author jackin
 * @Description
 * @create 2022-12-12 15:18
 **/
@TapConnectorClass("aliyun-adb-mysql-spec.json")
public class AliyunADBMySQLConnector extends MysqlConnector {
	public static final String TAG = AliyunADBMySQLConnector.class.getSimpleName();
	private MysqlWriter mysqlWriter;
	private MysqlJdbcContext aliyunADBJdbcContext;

	@Override
	public void onStop(TapConnectionContext connectionContext) {
		super.onStop(connectionContext);
		try {
			Optional.ofNullable(this.mysqlWriter).ifPresent(MysqlWriter::onDestroy);
		} catch (Exception ignored) {
		}
		if (null != aliyunADBJdbcContext) {
			try {
				this.aliyunADBJdbcContext.close();
				this.aliyunADBJdbcContext = null;
			} catch (Exception e) {
				TapLogger.error(TAG, "Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
			}
		}
	}

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
		ConnectionOptions connectionOptions = ConnectionOptions.create();
		CommonDbConfig commonDbConfig = new CommonDbConfig();
		//commonDbConfig.set__connectionType(databaseContext.getConnectionConfig().getString("__connectionType"));
		try (
				MysqlConnectionTest mysqlConnectionTest = new MysqlConnectionTest(new MysqlJdbcContext(databaseContext),
						databaseContext, consumer, commonDbConfig, connectionOptions)
		) {
			mysqlConnectionTest.testOneByOne();
			return connectionOptions;
		}
	}

	private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
		WriteListResult<TapRecordEvent> writeListResult = this.mysqlWriter.write(tapConnectorContext, tapTable, tapRecordEvents);
		consumer.accept(writeListResult);
	}
}
