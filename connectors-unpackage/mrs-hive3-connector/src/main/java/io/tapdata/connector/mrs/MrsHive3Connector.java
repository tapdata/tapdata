package io.tapdata.connector.mrs;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.connector.hive.HiveConnector;
import io.tapdata.connector.mrs.config.MrsHive3Config;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

@TapConnectorClass("spec_mrshive3.json")
public class MrsHive3Connector extends HiveConnector {

    @Override
    public void onStart(TapConnectionContext connectionContext) {
        isConnectorStarted(connectionContext, connectorContext -> {
            firstConnectorId = (String) connectorContext.getStateMap().get("firstConnectorId");
            if (EmptyKit.isNull(firstConnectorId)) {
                firstConnectorId = connectionContext.getId();
                connectorContext.getStateMap().put("firstConnectorId", firstConnectorId);
            }
        });
        hiveConfig = new MrsHive3Config(firstConnectorId).load(connectionContext.getConnectionConfig());
        hiveJdbcContext = new MrsHive3JdbcContext((MrsHive3Config) hiveConfig);
        commonDbConfig = hiveConfig;
        jdbcContext = hiveJdbcContext;
        commonSqlMaker = new CommonSqlMaker('`');
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
        consumer.accept(testItem(TestItem.ITEM_LOGIN, TestItem.RESULT_SUCCESSFULLY));
        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
        return null;
    }

}
