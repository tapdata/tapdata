package io.tapdata.connector.mariadb;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.mysql.MysqlConnectionTest;
import io.tapdata.connector.mysql.MysqlJdbcContext;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import org.apache.commons.lang3.StringUtils;

import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;


public class MariadbConnectionTest extends MysqlConnectionTest {

    private static final String MARIADB_VERSION_5 = "5";

    private static final String MARIADB_VERSION_10 = "10";


    public MariadbConnectionTest(MysqlJdbcContext mysqlJdbcContext, TapConnectionContext tapConnectionContext,
                                 Consumer<TestItem> consumer, CommonDbConfig commonDbConfig, ConnectionOptions connectionOptions) {
        super(mysqlJdbcContext,tapConnectionContext,consumer,commonDbConfig,connectionOptions);
    }

    /**
     * 检查mariadb版本。只支持5.x和10.x版本
     *
     * @return
     */
    @Override
    public Boolean testVersion() {
        try {
            String version = mysqlJdbcContext.getMysqlVersion();
            String versionMsg = "version: " + version;
            if (StringUtils.isNotBlank(version)) {
                if (!version.startsWith(MARIADB_VERSION_5) && !version.startsWith(MARIADB_VERSION_10)) {
                    consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, versionMsg + " not supported well"));
                    return true;
                }
            }
        } catch (Throwable e) {
            consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
        consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY));
        return true;

    }


}
