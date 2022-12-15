package io.tapdata.connector.adb;

import io.tapdata.connector.mysql.MysqlConnector;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.function.Consumer;

/**
 * @author jackin
 * @Description
 * @create 2022-12-12 15:18
 **/
@TapConnectorClass("aliyun-adb-mysql-spec.json")
public class AliyunADBMySQLConnector extends MysqlConnector {

	@Override
	public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
		tapConnectionContext.getSpecification().setId("mysql");
		super.onStart(tapConnectionContext);
	}

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
		super.registerCapabilities(connectorFunctions, codecRegistry);
		connectorFunctions.supportWriteRecord(null);
	}

	@Override
	public ConnectionOptions connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) {
		databaseContext.getSpecification().setId("mysql");
		return super.connectionTest(databaseContext, consumer);
	}
}
