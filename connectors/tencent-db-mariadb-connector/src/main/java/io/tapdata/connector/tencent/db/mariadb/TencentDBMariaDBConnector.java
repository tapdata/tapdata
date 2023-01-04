package io.tapdata.connector.tencent.db.mariadb;

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
@TapConnectorClass("tencent-db-mariadb-spec.json")
public class TencentDBMariaDBConnector extends MysqlConnector {

	@Override
	public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
		tapConnectionContext.getConnectionConfig().put("protocolType", "mysql");
		super.onStart(tapConnectionContext);
	}

	@Override
	public ConnectionOptions connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) {
		databaseContext.getConnectionConfig().put("protocolType", "mysql");
		return super.connectionTest(databaseContext, consumer);
	}
}
