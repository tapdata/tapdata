package io.tapdata.js.connector.server.function.support;

import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.functions.connector.source.RawDataCallbackFilterFunctionV2;

public class JSRawDataCallbackFunction extends FunctionBase implements FunctionSupport<RawDataCallbackFilterFunctionV2> {
    private JSRawDataCallbackFunction() {
        super();
        super.functionName = JSFunctionNames.BatchReadFunction;
    }

    @Override
    public RawDataCallbackFilterFunctionV2 function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) return null;
        return null;
    }

    public static RawDataCallbackFilterFunctionV2 create(LoadJavaScripter loadJavaScripter) {
        return new JSRawDataCallbackFunction().function(loadJavaScripter);
    }
}
