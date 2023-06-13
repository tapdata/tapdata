package io.tapdata.js.connector;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.APIFactoryImpl;
import io.tapdata.common.support.APIFactory;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.js.connector.base.JsUtil;
import io.tapdata.js.connector.base.TapConfigContext;
import io.tapdata.js.connector.base.TapConnectorLog;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.decorator.APIFactoryDecorator;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.support.BaseConnectionTestFunction;
import io.tapdata.js.connector.server.function.support.BaseDiscoverSchemaFunction;
import io.tapdata.js.connector.server.function.support.BaseTableCountFunction;
import io.tapdata.js.connector.server.function.support.BaseUpdateTokenFunction;
import io.tapdata.js.connector.server.function.support.JSBatchCountFunction;
import io.tapdata.js.connector.server.function.support.JSBatchReadFunction;
import io.tapdata.js.connector.server.function.support.JSCommandFunction;
import io.tapdata.js.connector.server.function.support.JSCreateTableV2Function;
import io.tapdata.js.connector.server.function.support.JSExecuteCommandFunction;
import io.tapdata.js.connector.server.function.support.JSRawDataCallbackFunction;
import io.tapdata.js.connector.server.function.support.JSStreamReadFunction;
import io.tapdata.js.connector.server.function.support.JSTimestampToStreamOffsetFunction;
import io.tapdata.js.connector.server.function.support.JSWriteRecordFunction;
import io.tapdata.js.connector.server.inteceptor.JSAPIInterceptorConfig;
import io.tapdata.js.connector.server.inteceptor.JSAPIResponseInterceptor;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class JSConnector extends ConnectorBase {
    private static final String TAG = JSConnector.class.getSimpleName();
    protected LoadJavaScripter javaScripter;
    protected Map<String, Object> apiParam = new HashMap<>();
    protected APIFactory apiFactory = new APIFactoryImpl();
    public static final TapConfigContext tapConfig = new TapConfigContext();
    //private CacheContext cacheContext = new CacheContext();

    protected final AtomicBoolean isAlive = new AtomicBoolean(true);

    public static final Object execLock = new Object();


    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        this.isAlive.set(true);
        if (connectionContext instanceof TapConnectorContext) {
            DataMap configMap = Optional.ofNullable(connectionContext.getConnectionConfig()).orElse(new DataMap());
            DataMap nodeConfig = connectionContext.getNodeConfig();
            if (Objects.nonNull(nodeConfig) && !nodeConfig.isEmpty()) {
                configMap.putAll(nodeConfig);
            }
            this.instanceScript(connectionContext, configMap);
        } else {
            this.instanceScript(connectionContext, Optional.ofNullable(connectionContext.getConnectionConfig()).orElse(new DataMap()));
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        this.isAlive.set(false);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        this.instanceScript(null, null);
        connectorFunctions.supportStreamRead(FunctionSupport.function(this.javaScripter, script -> JSStreamReadFunction.create(script, this.isAlive)))
                .supportWriteRecord(FunctionSupport.function(this.javaScripter, script -> JSWriteRecordFunction.create(this.isAlive).write(script)))
                .supportBatchRead(FunctionSupport.function(this.javaScripter, script -> JSBatchReadFunction.create(script, this.isAlive)))
                .supportBatchCount(FunctionSupport.function(this.javaScripter, JSBatchCountFunction::create))
                .supportRawDataCallbackFilterFunctionV2(FunctionSupport.function(this.javaScripter, JSRawDataCallbackFunction::create))
                .supportCreateTableV2(FunctionSupport.function(this.javaScripter, JSCreateTableV2Function::create))
                .supportTimestampToStreamOffset(FunctionSupport.function(this.javaScripter, JSTimestampToStreamOffsetFunction::create))
                .supportCommandCallbackFunction(FunctionSupport.function(this.javaScripter, JSCommandFunction::create))
                .supportExecuteCommandFunction(FunctionSupport.function(this.javaScripter, JSExecuteCommandFunction::create))
        ;
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tablesFilter, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        BaseDiscoverSchemaFunction.discover(this.javaScripter).invoker(connectionContext, consumer);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        this.instanceScript(connectionContext, Optional.ofNullable(connectionContext.getConnectionConfig()).orElse(new DataMap()));
        return BaseConnectionTestFunction.connection(this.javaScripter).test(connectionContext, consumer);
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return BaseTableCountFunction.tableCount(this.javaScripter).get(connectionContext);
    }

    protected void instanceScript(TapConnectionContext connectionContext, Map<String, Object> configMap) {
        if (Objects.isNull(this.javaScripter)) {
            synchronized (this) {
                if (Objects.isNull(this.javaScripter)) {
                    this.javaScripter = LoadJavaScripter.loader("", JS_FLOODER);
                    if (Objects.nonNull(connectionContext)) {
                        DataMap connectionConfig = connectionContext.getConnectionConfig();
                        if (Objects.nonNull(connectionConfig)) {
                            String apiType = connectionConfig.getString("apiType");
                            if (Objects.isNull(apiType)) apiType = "POST_MAN";
                            String jsonTxt = connectionConfig.getString("jsonTxt");
                            if (Objects.isNull(jsonTxt)) {
                                TapLogger.error(TAG, "API JSON must be not null or not empty. ");
                            }
                            try {
                                toJson(jsonTxt);
                            } catch (Exception e) {
                                TapLogger.error(TAG, "API JSON only JSON format. ");
                            }
                        }
                    }
                }
            }
        }
        if (!this.javaScripter.hasLoad()) {
            synchronized (this) {
                if (!this.javaScripter.hasLoad()) {
                    JSAPIInterceptorConfig config = JSAPIInterceptorConfig.config();
                    JSAPIResponseInterceptor interceptor = JSAPIResponseInterceptor.create(config).configMap(configMap);
                    interceptor.updateToken(BaseUpdateTokenFunction.create(this.javaScripter, connectionContext));
                    APIFactoryDecorator factory = new APIFactoryDecorator(this.apiFactory).interceptor(interceptor);
                    this.javaScripter.scriptEngine().put("tapAPI", factory);
                    //this.javaScripter.scriptEngine().put("tapCache", this.cacheContext);
                    this.javaScripter.scriptEngine().put("tapUtil", new JsUtil());
                    this.javaScripter.scriptEngine().put("nodeIsAlive", isAlive);
                    this.javaScripter.scriptEngine().put("_tapConfig_", configMap);
                    this.javaScripter.scriptEngine().put("tapConfig", tapConfig);
                    this.javaScripter.scriptEngine().put("tapLog",
                            Objects.nonNull(connectionContext) ?
                                    new TapConnectorLog(Optional.ofNullable(connectionContext.getLog()).orElse(new TapLog()))
                                    : new TapLog()
                            );
                    this.load();
                }
            }
        } else {
            JSAPIInterceptorConfig config = JSAPIInterceptorConfig.config();
            JSAPIResponseInterceptor interceptor = JSAPIResponseInterceptor.create(config).configMap(configMap);
            if (Objects.nonNull(connectionContext)) {
                interceptor.updateToken(BaseUpdateTokenFunction.create(this.javaScripter, connectionContext));
                this.javaScripter.scriptEngine().put("tapLog", new TapConnectorLog(connectionContext.getLog()));
            }
            Object tapAPI = this.javaScripter.scriptEngine().get("tapAPI");
            if (Objects.isNull(tapAPI) || !(tapAPI instanceof APIFactoryDecorator)) {
                APIFactoryDecorator factory = new APIFactoryDecorator(this.apiFactory);
                this.javaScripter.scriptEngine().put("tapAPI", factory.interceptor(interceptor));
            } else {
                this.javaScripter.scriptEngine().put("tapAPI", ((APIFactoryDecorator) tapAPI).interceptor(interceptor));
            }
            Object configMapObj = this.javaScripter.scriptEngine().get("_tapConfig_");
            if (Objects.nonNull(configMapObj) && ((Map<String, Object>) configMapObj).isEmpty() && !configMap.isEmpty()) {
                ((Map<String, Object>) configMapObj).putAll(configMap);
            } else {
                this.javaScripter.scriptEngine().put("_tapConfig_", configMap);
            }
        }
    }

    public static final String JS_FLOODER = "connectors-javascript";

    protected void load() {
        try {
            ClassLoader classLoader = JSConnector.class.getClassLoader();
            Enumeration<URL> resources = classLoader.getResources(JS_FLOODER + "/");
            this.javaScripter.load(resources);
        } catch (Exception error) {
            throw new CoreException(error.getMessage());
        }
    }
}
