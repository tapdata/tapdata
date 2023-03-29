package io.tapdata.connector.adb;

import io.tapdata.connector.postgres.PostgresConnector;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

/**
 * @author jackin
 * @Description
 * @create 2022-12-12 15:18
 **/
@TapConnectorClass("tencent-db-postgres-spec.json")
public class TencentDBPostgresConnector extends PostgresConnector {

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        postgresConfig = (PostgresConfig) new PostgresConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(postgresConfig.getConnectionString());
        try (
                TencentDBPostgresTest tencentDBPostgresTest = new TencentDBPostgresTest(postgresConfig, consumer).initContext()
        ) {
            tencentDBPostgresTest.testOneByOne();
            return connectionOptions;
        }
    }

}
