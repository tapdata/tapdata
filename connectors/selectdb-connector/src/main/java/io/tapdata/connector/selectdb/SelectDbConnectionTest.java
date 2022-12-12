package io.tapdata.connector.selectdb;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.selectdb.constant.SelectDbTestItem;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

/**
 * Author:Skeet
 * Date: 2022/12/9
 **/

public class SelectDbConnectionTest extends CommonDbTest {
    protected static final String TAG = SelectDbConnectionTest.class.getSimpleName();

    protected TapConnectionContext tapConnectionContext;

    protected SelectDbJdbcContext selectDbJdbcContext;

    protected ConnectionOptions connectionOptions;

    public SelectDbConnectionTest(TapConnectionContext tapConnectionContext, SelectDbJdbcContext selectDbJdbcContext,
                                  Consumer<TestItem> consumer, CommonDbConfig commonDbConfig, ConnectionOptions connectionOptions) {
        super(commonDbConfig, consumer);
        this.tapConnectionContext = tapConnectionContext;
        this.selectDbJdbcContext = selectDbJdbcContext;
        this.connectionOptions = connectionOptions;
    }

    @Override
    protected Boolean testHostPort() {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String host = String.valueOf(connectionConfig.get("host"));
        int port = ((Number) connectionConfig.get("port")).intValue();
        try {
            NetUtil.validateHostPortWithSocket(host, port);
            consumer.accept(testItem(SelectDbTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY));
            return true;
        } catch (IOException e) {
            consumer.accept(testItem(SelectDbTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }

    @Override
    public Boolean testConnect() {
        try (
                Connection connection = selectDbJdbcContext.getConnection();
        ) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
            return true;
        } catch (Exception e) {
            if (e instanceof SQLException) {
                String errMsg = e.getMessage();
                if (errMsg.contains("using password")) {
                    String password = selectDbJdbcContext.getTapConnectionContext().getConnectionConfig().getString("password");
                    if (StringUtils.isNotEmpty(password)) {
                        errMsg = "password or username is error ,please check";
                    } else {
                        errMsg = "password is empty,please enter password";
                    }
                    consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, errMsg));
                    return false;

                }
            }
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }
}
