package io.tapdata.js.connector;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.APIFactoryImpl;
import io.tapdata.common.support.core.ConnectorLog;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.iengine.ScriptEngineInstance;
import io.tapdata.js.connector.server.decorator.APIFactoryDecorator;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.support.*;
import io.tapdata.js.connector.server.inteceptor.JSAPIInterceptorConfig;
import io.tapdata.js.connector.server.inteceptor.JSAPIResponseInterceptor;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.common.support.APIFactory;
import io.tapdata.common.support.APIInvoker;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class JSConnector extends ConnectorBase {
    private static final String TAG = JSConnector.class.getSimpleName();
    private LoadJavaScripter javaScripter;
    private APIInvoker apiInvoker ;
    private Map<String,Object> apiParam = new HashMap<>();
    private APIFactory apiFactory = new APIFactoryImpl();

    private final AtomicBoolean isAlive = new AtomicBoolean(true);
    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        this.instanceScript(connectionContext);
        this.isAlive.set(true);

    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        this.isAlive.set(false);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        this.instanceScript(null);
        connectorFunctions.supportStreamRead(FunctionSupport.function(javaScripter, script->JSStreamReadFunction.create(script,this.isAlive)))
            .supportWriteRecord(FunctionSupport.function(javaScripter, script->JSWriteRecordFunction.create(script,this.isAlive)))
            .supportBatchRead(FunctionSupport.function(javaScripter, script->JSBatchReadFunction.create(script,this.isAlive)))
            .supportBatchCount(FunctionSupport.function(javaScripter, JSBatchCountFunction::create))
            .supportRawDataCallbackFilterFunctionV2(FunctionSupport.function(javaScripter, JSRawDataCallbackFunction::create))
            .supportCreateTableV2(FunctionSupport.function(javaScripter, JSCreateTableV2Function::create));
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tablesFilter, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable{
        this.instanceScript(connectionContext);
        Object map = this.javaScripter.invoker("emptyMap");
        Object map1 = this.javaScripter.invoker("testMap");
        Object testString = this.javaScripter.invoker("testString");
        BaseDiscoverSchemaFunction.discover(this.javaScripter).invoker(connectionContext,consumer);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        this.instanceScript(connectionContext);
        return BaseConnectionTestFunction.connection(this.javaScripter).test(connectionContext,consumer);
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        this.instanceScript(connectionContext);
        return BaseTableCountFunction.tableCount(this.javaScripter).get(connectionContext);
    }

    private void instanceScript(TapConnectionContext connectionContext){
        ScriptEngineInstance engineInstance = ScriptEngineInstance.instance();
        if(Objects.isNull(this.javaScripter)) {
            this.javaScripter = engineInstance.script();
            if (Objects.nonNull(connectionContext)){
                DataMap connectionConfig = connectionContext.getConnectionConfig();
                if (Objects.nonNull(connectionConfig)) {
                    String apiType = connectionConfig.getString("apiType");
                    if (Objects.isNull(apiType)) apiType = "POST_MAN";
                    String jsonTxt = connectionConfig.getString("jsonTxt");
                    if (Objects.isNull(jsonTxt)){
                        TapLogger.error(TAG,"API JSON must be not null or not empty. ");
                    }
                    try {
                        toJson(jsonTxt);
                    }catch (Exception e){
                        TapLogger.error(TAG,"API JSON only JSON format. ");
                    }
                }
            }
        }
        JSAPIInterceptorConfig config = JSAPIInterceptorConfig.config();
        APIFactoryDecorator factory = new APIFactoryDecorator(apiFactory).interceptor(JSAPIResponseInterceptor.create(config, apiInvoker));
        this.javaScripter.scriptEngine().put("tapAPI",factory);
        this.javaScripter.scriptEngine().put("log",new ConnectorLog());
        //this.javaScripter.bindingGlobal("tapFunctions",new ArrayList<>());
//        try {
//            this.javaScripter.scriptEngine().eval("var tapAPI = Java.type(\"io.tapdata.js.connector.server.decorator.APIFactoryDecorator\");");
//        }catch (Exception e){
//
//        }
        engineInstance.loadScript();
    }
}
