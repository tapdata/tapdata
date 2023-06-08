package io.tapdata.http.receiver;

import io.tapdata.http.entity.ConnectionConfig;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.tapdata.base.ConnectorBase.testItem;

/**
 * @author GavinXiao
 * @description ConnectionTest create by Gavin
 * @create 2023/5/17 17:32
 **/
public class ConnectionTest {
    private ConnectionConfig config;
    private static final String TABLE_NAME_REGEX = ".*[\\s`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？\\\\]+.*";

    public static ConnectionTest create(ConnectionConfig config) {
        return new ConnectionTest().config(config);
    }

    public ConnectionTest config(ConnectionConfig config) {
        this.config = config;
        return this;
    }

    public TestItem testTableName() {
        try {
            String tableName = config.getTableName();
            if (null == tableName || "".equals(tableName.trim())) {
                return testItem(TestHttpItem.TEST_TABLE_NAME.content(), TestItem.RESULT_FAILED, "Table name can not be empty.");
            }
            Matcher m = (Pattern.compile(TABLE_NAME_REGEX)).matcher(tableName);
            boolean matches = m.matches();
            return testItem(TestHttpItem.TEST_TABLE_NAME.content(), matches ? TestItem.RESULT_SUCCESSFULLY_WITH_WARN : TestItem.RESULT_SUCCESSFULLY , !matches ? "" : "There are some special characters in the table name. Please confirm if necessary, and ignore this prompt if necessary.");
        } catch (Exception e) {
            return testItem(TestHttpItem.TEST_TABLE_NAME.content(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    public TestItem testScript() {
        try {
            //String originalScript = config.originalScript();
            return testItem(TestHttpItem.TEST_EVENT_SCRIPT.content(), TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            return testItem(TestHttpItem.TEST_EVENT_SCRIPT.content(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    public TestItem testHookUrl() {
        try {
            String hookUrl = config.getHookUrl();
            if (null == hookUrl || "".equals(hookUrl.trim())) {
                return testItem(TestHttpItem.TEST_HOOK_URL.content(), TestItem.RESULT_FAILED, " Please check if a WebHook URL has been generated and go to a third-party platform to configure WebHook ");
            }
            return testItem(TestHttpItem.TEST_HOOK_URL.content(), TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            return testItem(TestHttpItem.TEST_HOOK_URL.content(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    static enum TestHttpItem {
        TEST_TABLE_NAME("Check Table Name"),
        TEST_HOOK_URL("Check if the link is generated"),
        TEST_EVENT_SCRIPT("Check if the script is written");
        String content;

        TestHttpItem(String content) {
            this.content = content;
        }

        public String content() {
            return this.content;
        }
    }
}
