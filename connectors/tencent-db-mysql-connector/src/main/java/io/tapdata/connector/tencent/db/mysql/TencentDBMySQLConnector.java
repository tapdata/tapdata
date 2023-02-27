package io.tapdata.connector.tencent.db.mysql;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.mysql.MysqlConnectionTest;
import io.tapdata.connector.mysql.MysqlConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

/**
 * @author jackin
 * @Description
 * @create 2022-12-15 11:42
 **/
@TapConnectorClass("tencent-db-mysql-spec.json")
public class TencentDBMySQLConnector extends MysqlConnector {
	private TapConnectionContext tapConnectionContext;

	@Override
	public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
		this.tapConnectionContext=tapConnectionContext;
		tapConnectionContext.getConnectionConfig().put("protocolType", "mysql");
		super.onStart(tapConnectionContext);
	}

	@Override
	public ConnectionOptions connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) {

		databaseContext.getConnectionConfig().put("protocolType", "mysql");
		ConnectionOptions connectionOptions = ConnectionOptions.create();
		CommonDbConfig commonDbConfig = new CommonDbConfig();
		commonDbConfig.set__connectionType(databaseContext.getConnectionConfig().getString("__connectionType"));
		try (
				TencentDBMySQLConnectorTest tencentDBMySQLConnectorTest = new TencentDBMySQLConnectorTest(new MysqlJdbcContext(databaseContext),
						databaseContext, consumer, commonDbConfig, connectionOptions)
		) {
			tencentDBMySQLConnectorTest.testOneByOne();
			return connectionOptions;
		}
	}
}
