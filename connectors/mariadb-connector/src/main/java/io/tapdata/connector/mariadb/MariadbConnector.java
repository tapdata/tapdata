package io.tapdata.connector.mariadb;

import io.tapdata.connector.mysql.MysqlConnector;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;


@TapConnectorClass("spec_mariadb.json")
public class MariadbConnector extends MysqlConnector {

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        mysqlConfig = new MysqlConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(mysqlConfig.getConnectionString());
        try (
                MariadbTest mariadbTest = new MariadbTest(mysqlConfig, consumer)
        ) {
            mariadbTest.testOneByOne();
        }
        return connectionOptions;
    }
}

