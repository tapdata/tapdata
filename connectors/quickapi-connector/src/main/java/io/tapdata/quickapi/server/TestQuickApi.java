package io.tapdata.quickapi.server;

import io.tapdata.common.api.APIFactory;
import io.tapdata.common.api.APIInvoker;
import io.tapdata.common.api.APIResponse;
import io.tapdata.common.core.emun.TapApiTag;
import io.tapdata.common.support.APIFactoryImpl;
import io.tapdata.common.support.postman.entity.ApiMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.quickapi.server.enums.QuickApiTestItem;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static io.tapdata.base.ConnectorBase.testItem;
import static io.tapdata.base.ConnectorBase.toJson;

public class TestQuickApi extends QuickApiBase {
    public static TestQuickApi create(TapConnectionContext connectionContext){
        return new TestQuickApi(connectionContext);
    }
    public TestQuickApi(TapConnectionContext connectionContext){
        super(connectionContext);
        apiFactory = new APIFactoryImpl();
        invoker = apiFactory.loadAPI(super.config.jsonTxt(), super.config.apiType(), null);
        invoker.setAPIResponseInterceptor(QuickAPIResponseInterceptor.create(config,invoker));
    }

    private APIInvoker invoker;
    private APIFactory apiFactory;

    //检查JSON 格式是否正确
    public TestItem testJSON(){
        try {
            try {
                toJson(super.config.jsonTxt());
                return testItem(QuickApiTestItem.TEST_JSON_FORMAT.testName(), TestItem.RESULT_SUCCESSFULLY);
            }catch (Exception e){
                return testItem(QuickApiTestItem.TEST_JSON_FORMAT.testName(), TestItem.RESULT_FAILED,"API JSON only JSON format. ");
            }
        }catch (Exception e){
            return testItem(QuickApiTestItem.TEST_JSON_FORMAT.testName(),TestItem.RESULT_FAILED,e.getMessage());
        }
    }

    //是否含有 TAP_TABLE，有没有正确指定表名称，否则 ERROR，是否指明分页逻辑，否则 ERROR
    public TestItem testTapTableTag(){
        try {
            List<ApiMap.ApiEntity> tables = this.invoker.tableApis();
            if (tables.isEmpty()) {
                return testItem(QuickApiTestItem.TEST_TAP_TABLE.testName(), TestItem.RESULT_FAILED,"Please use TAP on the API document_ The TABLE format label specifies at least one table data add in for the data source.");
            }
            for (ApiMap.ApiEntity apiEntity : tables) {
                String tableName = TapApiTag.analysisTableName(apiEntity.name());
                if (!TapApiTag.hasPageStage(apiEntity.name())){
                    return testItem(
                        QuickApiTestItem.TEST_TAP_TABLE.testName(),
                        TestItem.RESULT_FAILED,
                        String.format("It is detected that the API with name %s and URL= %s specifies the table name \"%s\", but does not specify the table paging method.",apiEntity.name(),apiEntity.url(),tableName));
                }
            }
            return testItem(QuickApiTestItem.TEST_TAP_TABLE.testName(),TestItem.RESULT_SUCCESSFULLY);
        }catch (Exception e){
            return testItem(QuickApiTestItem.TEST_TAP_TABLE.testName(),TestItem.RESULT_FAILED,e.getMessage());
        }
    }

    //是否指定 access_token 变量的名称以及需要赋值的属性对应为哪个，否则 WARN
    //1. 解析是否含有标签上的token接口，
    //2. 是否在连接页面配置了access_token 的过期重置的状态
    //3. 是否声明了access_token变量名称的对应关系
    public TestItem testTokenConfig(){
        try {
            List<ApiMap.ApiEntity> tables = this.invoker.tableApis();
            if (!tables.isEmpty()) {
                if (tables.size()>1){
                    return testItem(QuickApiTestItem.TEST_TOKEN.testName(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,"The resolved API document does not use TAP_GET_TOKEN claims that the access TOKEN has obtained too many API.");
                }
                String tokenParams = config.tokenParams();
                if (Objects.isNull(tokenParams)){
                    return testItem(QuickApiTestItem.TEST_TOKEN.testName(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,"The API access token key value correspondence is not declared on the connection page. Please declare it manually if necessary.");
                }
                String expireStatus = config.expireStatus();
                if (Objects.isNull(expireStatus)){
                    return testItem(QuickApiTestItem.TEST_TOKEN.testName(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,"The API access token expiration status is not declared on the connection page. Please declare it manually if necessary.");
                }
                return testItem(QuickApiTestItem.TEST_TOKEN.testName(), TestItem.RESULT_SUCCESSFULLY);
            }
            return testItem(QuickApiTestItem.TEST_TOKEN.testName(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,"The resolved API document does not use TAP_GET_TOKEN declares the access token acquisition API. ");
        }catch (Exception e){
            return testItem(QuickApiTestItem.TEST_TOKEN.testName(),TestItem.RESULT_FAILED,e.getMessage());
        }
    }

    //依次检查能否依次调通这些除token获取以外的标记了的接口，否则 WARN
    public TestItem testApi(){
        try {
            List<ApiMap.ApiEntity> tables = this.invoker.tableApis();
            if (tables.isEmpty()) {
                return testItem(QuickApiTestItem.TEST_TAP_TABLE.testName(), TestItem.RESULT_FAILED,"Please use TAP on the API document_ The TABLE format label specifies at least one table data add in for the data source.");
            }
            int testApiIndex = 1;
            boolean testApiFailed = false;
            StringJoiner joiner = new StringJoiner(";\n");
            for (ApiMap.ApiEntity apiEntity : tables) {
                APIResponse http = invoker.invoke(apiEntity.name(), apiEntity.method(), this.invoker.variable(),true);
                StringBuilder builder = new StringBuilder();
                builder.append("(")
                        .append(testApiIndex++)
                        .append(")")
                        .append("Trial run API: name=")
                        .append(apiEntity.name())
//                        .append(", url=")
//                        .append(apiEntity.url())
//                        .append(", method=")
//                        .append(apiEntity.method())
                        .append(", running result:");
                if (Objects.isNull(http) || !Objects.equals(http.httpCode(),200)){
                    builder.append("[ERROR]").append(toJson(http.result())).append(".\n");
                    testApiFailed = true;
                }else {
                    builder.append("[SUCCEED]. \n");
                }
                joiner.add(builder.toString());
            }
            return testItem(QuickApiTestItem.DEBUG_API.testName(),testApiFailed ? TestItem.RESULT_FAILED : TestItem.RESULT_SUCCESSFULLY,joiner.toString());
        }catch (Exception e){
            return testItem(QuickApiTestItem.DEBUG_API.testName(),TestItem.RESULT_FAILED,e.getMessage());
        }
    }
}
