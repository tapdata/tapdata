package io.tapdata.js.connector;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.iengine.ScriptEngineInstance;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.support.*;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class JSConnector extends ConnectorBase {
    private static final String TAG = JSConnector.class.getSimpleName();
    private LoadJavaScripter javaScripter;

    private final AtomicBoolean isAlive = new AtomicBoolean(true);
    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        this.instanceScript();
        this.isAlive.set(true);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        this.isAlive.set(false);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        this.instanceScript();
        connectorFunctions.supportStreamRead(FunctionSupport.function(javaScripter, script->JSStreamReadFunction.create(script,this.isAlive)))
            .supportWriteRecord(FunctionSupport.function(javaScripter, script->JSWriteRecordFunction.create(script,this.isAlive)))
            .supportBatchRead(FunctionSupport.function(javaScripter, script->JSBatchReadFunction.create(script,this.isAlive)))
            .supportBatchCount(FunctionSupport.function(javaScripter, JSBatchCountFunction::create))
            .supportCreateTableV2(FunctionSupport.function(javaScripter, JSCreateTableV2Function::create));
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tablesFilter, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable{
        this.instanceScript();
        BaseDiscoverSchemaFunction.discover(this.javaScripter).invoker(connectionContext,consumer);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        this.instanceScript();
        return BaseConnectionTestFunction.connection(this.javaScripter).test(connectionContext,consumer);
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        this.instanceScript();
        return BaseTableCountFunction.tableCount(this.javaScripter).get(connectionContext);
    }

    private void instanceScript(){
        if(Objects.isNull(this.javaScripter)) {
            this.javaScripter = ScriptEngineInstance.instance().script();
        }
    }
}
