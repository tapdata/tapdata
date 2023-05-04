package io.tapdata.quickapi.server;

import io.tapdata.common.APIFactoryImpl;
import io.tapdata.common.support.APIFactory;
import io.tapdata.common.support.APIInvoker;
import io.tapdata.common.support.core.emun.TapApiTag;
import io.tapdata.common.support.entitys.APIEntity;
import io.tapdata.common.support.entitys.APIResponse;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.quickapi.server.enums.QuickApiTestItem;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.tapdata.base.ConnectorBase.testItem;
import static io.tapdata.base.ConnectorBase.toJson;

public class TestQuickApi extends QuickApiBase {
    public static TestQuickApi create(TapConnectionContext connectionContext) {
        return new TestQuickApi(connectionContext);
    }

    public TestQuickApi(TapConnectionContext connectionContext) {
        super(connectionContext);
        apiFactory = new APIFactoryImpl();
        invoker = apiFactory.loadAPI(super.config.jsonTxt(), null);
        invoker.setAPIResponseInterceptor(QuickAPIResponseInterceptor.create(config, invoker));
    }

    private APIInvoker invoker;
    private APIFactory apiFactory;

    //检查JSON 格式是否正确
    public TestItem testJSON() {
        try {
            try {
                toJson(super.config.jsonTxt());
                return testItem(QuickApiTestItem.TEST_JSON_FORMAT.testName(), TestItem.RESULT_SUCCESSFULLY);
            } catch (Exception e) {
                return testItem(QuickApiTestItem.TEST_JSON_FORMAT.testName(), TestItem.RESULT_FAILED, "API JSON only JSON format. ");
            }
        } catch (Exception e) {
            return testItem(QuickApiTestItem.TEST_JSON_FORMAT.testName(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    //是否含有 TAP_TABLE，有没有正确指定表名称，否则 ERROR，是否指明分页逻辑，否则 ERROR
    public TestItem testTapTableTag() {
        try {
            List<APIEntity> tables = this.invoker.tableApis();
            if (tables.isEmpty()) {
                return testItem(QuickApiTestItem.TEST_TAP_TABLE.testName(), TestItem.RESULT_FAILED, "Please use TAP on the API document_ The TABLE format label specifies at least one table data add in for the data source.");
            }
            for (APIEntity apiEntity : tables) {
                String tableName = TapApiTag.analysisTableName(apiEntity.name());
                if (!TapApiTag.hasPageStage(apiEntity.name())) {
                    return testItem(
                            QuickApiTestItem.TEST_TAP_TABLE.testName(),
                            TestItem.RESULT_FAILED,
                            String.format("It is detected that the API with name %s and URL= %s specifies the table name \"%s\", but does not specify the table paging method.", apiEntity.name(), apiEntity.url(), tableName));
                }
            }
            return testItem(QuickApiTestItem.TEST_TAP_TABLE.testName(), TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            return testItem(QuickApiTestItem.TEST_TAP_TABLE.testName(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    //是否指定 access_token 变量的名称以及需要赋值的属性对应为哪个，否则 WARN
    //1. 解析是否含有标签上的token接口，
    //2. 是否在连接页面配置了access_token 的过期重置的状态
    //3. 是否声明了access_token变量名称的对应关系
    public TestItem testTokenConfig() {
        try {
            List<APIEntity> tokens = this.invoker.tokenApis();
            if (!tokens.isEmpty()) {
                if (tokens.size() > 1) {
                    return testItem(QuickApiTestItem.TEST_TOKEN.testName(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "The resolved API document does not use TAP_GET_TOKEN claims that the access TOKEN has obtained too many API.");
                }
                String tokenParams = config.tokenParams();
                if (Objects.isNull(tokenParams)) {
                    return testItem(QuickApiTestItem.TEST_TOKEN.testName(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "The API access token key value correspondence is not declared on the connection page. Please declare it manually if necessary.");
                }
                String expireStatus = config.expireStatus();
                if (Objects.isNull(expireStatus)) {
                    return testItem(QuickApiTestItem.TEST_TOKEN.testName(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "The API access token expiration status is not declared on the connection page. Please declare it manually if necessary.");
                }
                return testItem(QuickApiTestItem.TEST_TOKEN.testName(), TestItem.RESULT_SUCCESSFULLY);
            }
            return testItem(QuickApiTestItem.TEST_TOKEN.testName(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "The resolved API document does not use TAP_GET_TOKEN declares the access token acquisition API. ");
        } catch (Exception e) {
            return testItem(QuickApiTestItem.TEST_TOKEN.testName(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    //依次检查能否依次调通这些除token获取以外的标记了的接口，否则 WARN
    public List<TestItem> testApi() {
        List<TestItem> tests = new ArrayList<>();
        String apiName = "";
        try {
            List<APIEntity> tables = this.invoker.tableApis();
            if (tables.isEmpty()) {
                tests.add(testItem(QuickApiTestItem.TEST_TAP_TABLE.testName(), TestItem.RESULT_FAILED, "Please use TAP on the API document_ The TABLE format label specifies at least one table data add in for the data source."));
                return tests;
            }
            int testApiIndex = 1;
            for (APIEntity apiEntity : tables) {
                apiName = apiEntity.name();
                boolean testApiFailed = false;
                APIResponse http = invoker.invoke(apiName, this.invoker.variable(), apiEntity.method(), true);
                StringBuilder builder = new StringBuilder();
                builder.append("(")
                        .append(testApiIndex++)
                        .append(")")
                        .append("Trial run API: name=")
                        .append(apiName)
                        //.append(", url=")
                        //.append(apiEntity.url())
                        //.append(", method=")
                        //.append(apiEntity.method())
                        .append(", running result:");
                if (Objects.isNull(http) || 200 < http.httpCode() || http.httpCode() >= 300) {
                    builder.append("[ERROR]code=").append(http.httpCode())
                            .append(",result=").append(toJson(http.result())).append(".\n");
                    testApiFailed = true;
                } else {
                    builder.append("[SUCCEED]. \n");
                }
                tests.add(testItem(String.format(QuickApiTestItem.DEBUG_API.testName(),apiName), testApiFailed ? TestItem.RESULT_FAILED : TestItem.RESULT_SUCCESSFULLY, builder.toString()));
            }
        } catch (Exception e) {
            tests.add(testItem(String.format(QuickApiTestItem.DEBUG_API.testName(),apiName), TestItem.RESULT_FAILED, e.getMessage()));
        }
        return tests;
    }
}
