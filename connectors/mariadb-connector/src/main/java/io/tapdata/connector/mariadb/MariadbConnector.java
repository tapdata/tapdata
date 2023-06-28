package io.tapdata.connector.mariadb;

import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.connector.mysql.MysqlConnector;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.List;
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
        List<Capability> ddlCapabilities = DDLFactory.getCapabilities(DDLParserType.MYSQL_CCJ_SQL_PARSER);
        ddlCapabilities.forEach(connectionOptions::capability);
        return connectionOptions;
    }
}

