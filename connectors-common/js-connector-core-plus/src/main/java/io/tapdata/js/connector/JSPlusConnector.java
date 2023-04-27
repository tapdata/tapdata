package io.tapdata.js.connector;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.support.JSBatchCountFunction;
import io.tapdata.js.connector.server.function.support.JSBatchReadFunction;
import io.tapdata.js.connector.server.function.support.JSCommandFunction;
import io.tapdata.js.connector.server.function.support.JSConnectorWebsiteFunction;
import io.tapdata.js.connector.server.function.support.JSCreateTableV2Function;
import io.tapdata.js.connector.server.function.support.JSRawDataCallbackFunction;
import io.tapdata.js.connector.server.function.support.JSStreamReadFunction;
import io.tapdata.js.connector.server.function.support.JSTableWebsiteFunction;
import io.tapdata.js.connector.server.function.support.JSTimestampToStreamOffsetFunction;
import io.tapdata.js.connector.server.function.support.JSWriteRecordFunction;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

@TapConnectorClass("spec.json")
public class JSPlusConnector extends JSConnector {
    private static final String TAG = JSPlusConnector.class.getSimpleName();

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        super.instanceScript(null, null);
        connectorFunctions.supportStreamRead(FunctionSupport.function(this.javaScripter, script -> JSStreamReadFunction.create(script, this.isAlive)))
                .supportWriteRecord(FunctionSupport.function(this.javaScripter, script -> JSWriteRecordFunction.create(this.isAlive).write(script)))
                .supportBatchRead(FunctionSupport.function(this.javaScripter, script -> JSBatchReadFunction.create(script, this.isAlive)))
                .supportBatchCount(FunctionSupport.function(this.javaScripter, JSBatchCountFunction::create))
                .supportRawDataCallbackFilterFunctionV2(FunctionSupport.function(this.javaScripter, JSRawDataCallbackFunction::create))
                .supportCreateTableV2(FunctionSupport.function(this.javaScripter, JSCreateTableV2Function::create))
                .supportTimestampToStreamOffset(FunctionSupport.function(this.javaScripter, JSTimestampToStreamOffsetFunction::create))
                .supportConnectorWebsite(FunctionSupport.function(this.javaScripter, JSConnectorWebsiteFunction::create))
                .supportTableWebsite(FunctionSupport.function(this.javaScripter, JSTableWebsiteFunction::create))
                .supportCommandCallbackFunction(FunctionSupport.function(this.javaScripter, JSCommandFunction::create))
        ;
    }
}
