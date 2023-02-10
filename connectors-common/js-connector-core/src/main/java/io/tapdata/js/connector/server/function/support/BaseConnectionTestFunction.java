package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.js.connector.JSConnector;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

/**
 * testResult = [
 * {
 * "test":"Test Api 1 which is the test item' title. ",
 * "code":0,//default0(0:warn,1:succeed,-1:error),
 * "result":"there is a warn message for this test item."
 * }
 * ]
 */
public class BaseConnectionTestFunction extends FunctionBase {
    private static final String TAG = BaseConnectionTestFunction.class.getSimpleName();

    private BaseConnectionTestFunction() {
        super();
        super.functionName = JSFunctionNames.CONNECTION_TEST;
    }

    public static BaseConnectionTestFunction connection(LoadJavaScripter script) {
        BaseConnectionTestFunction function = new BaseConnectionTestFunction();
        function.javaScripter(script);
        return function;
    }

    public static final String TEST_RESULT_TITLE_KEY = "test";
    public static final String TEST_RESULT_CODE_KEY = "code";
    public static final String TEST_RESULT_MSG_KEY = "result";

    public ConnectionOptions test(TapConnectionContext context, Consumer<TestItem> consumer) {
        ConnectionOptions options = new ConnectionOptions();
        if (this.javaScripter.functioned(functionName.jsName())) {
            if (Objects.isNull(context)) {
                consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "TapConnectorContext cannot not be empty."));
                return options;
            }
            Object invoker;
            synchronized (JSConnector.execLock) {
                invoker = super.javaScripter.invoker(JSFunctionNames.CONNECTION_TEST.jsName(), Optional.ofNullable(context.getConnectionConfig()).orElse(new DataMap()));
            }
            try {
                List<Map<String, Object>> testList = (List<Map<String, Object>>) invoker;
                testList.stream().filter(map -> {
                    if (Objects.isNull(map)) return false;
                    Object titleObj = map.get(TEST_RESULT_TITLE_KEY);
                    Object codeObj = map.get(TEST_RESULT_TITLE_KEY);
                    if (Objects.isNull(titleObj) || Objects.isNull(codeObj)) {
                        TapLogger.info(TAG,
                                String.format("A connection test result that does not conform to the rule was found.The connection test items shall be a list composed of [%s, %s, %s], and %s and %s is required. "
                                        , TEST_RESULT_TITLE_KEY
                                        , TEST_RESULT_CODE_KEY
                                        , TEST_RESULT_MSG_KEY
                                        , TEST_RESULT_TITLE_KEY
                                        , TEST_RESULT_CODE_KEY));
                    }
                    return Objects.nonNull(titleObj) && Objects.nonNull(codeObj);
                }).forEach(map -> {
                    Object msgObj = map.get(TEST_RESULT_MSG_KEY);
                    String title = String.valueOf(map.get(TEST_RESULT_TITLE_KEY));
                    consumer.accept(testItem(title, this.result(map.get(TEST_RESULT_CODE_KEY)), Objects.isNull(msgObj) ? "" : String.valueOf(msgObj)));
                });
            } catch (Exception e) {
                TapLogger.info(TAG, String.format("The connection test items shall be a list composed of [%s, %s, %s], and %s and %s is required. "
                        , TEST_RESULT_TITLE_KEY
                        , TEST_RESULT_CODE_KEY
                        , TEST_RESULT_MSG_KEY
                        , TEST_RESULT_TITLE_KEY
                        , TEST_RESULT_CODE_KEY));
            }
        } else {
            consumer.accept(this.unableTestFunctionImplement());
        }
        return options;
    }

    public TestItem unableTestFunctionImplement() {
        return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, "Connection test has not been implemented yet. ");
    }

    public static final int ERROR_CODE = -1;
    public static final int WARN_CODE = 0;
    public static final int SUCCEED_CODE = 1;

    public int result(Object code) {
        if (Objects.isNull(code)) return TestItem.RESULT_SUCCESSFULLY_WITH_WARN;
        int codeNum = WARN_CODE;
        try {
            codeNum = (Integer) code;
        } catch (Exception ignore) {
        }
        return codeNum == ERROR_CODE ? TestItem.RESULT_FAILED : (codeNum == SUCCEED_CODE ? TestItem.RESULT_SUCCESSFULLY : TestItem.RESULT_SUCCESSFULLY_WITH_WARN);
    }
}
