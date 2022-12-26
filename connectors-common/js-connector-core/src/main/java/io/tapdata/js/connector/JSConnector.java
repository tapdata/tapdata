package io.tapdata.js.connector;

import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.iengine.ScriptEngineInstance;
import io.tapdata.js.connector.server.function.support.*;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.base.ConnectorBase;

import java.util.function.Consumer;
import java.util.*;

@TapConnectorClass("spec.json")
public class JSConnector extends ConnectorBase {
    private static final String TAG = JSConnector.class.getSimpleName();
    private LoadJavaScripter javaScripter;

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        this.instanceScript();
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {

    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        this.instanceScript();
        connectorFunctions.supportStreamRead(FunctionSupport.function(javaScripter, JSStreamReadFunction::create))
            .supportWriteRecord(FunctionSupport.function(javaScripter, JSWriteRecordFunction::create))
            .supportBatchRead(FunctionSupport.function(javaScripter, JSBatchReadFunction::create))
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
