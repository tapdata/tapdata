package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;

import java.util.List;
import java.util.Objects;

public class JSStreamReadFunction extends FunctionBase implements FunctionSupport<StreamReadFunction> {
    JSStreamReadFunction(){
        super();
        super.functionName = JSFunctionNames.StreamReadFunction;
    }
    @Override
    public StreamReadFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) return null;
        return this::streamRead;
    }

    private void streamRead(TapConnectorContext context, List<String> tables, Object offset, int readSize, StreamReadConsumer consumer) {
        if (Objects.isNull(context)){
            throw new CoreException("TapConnectorContext must not be null or not be empty.");
        }
        if(Objects.isNull(tables)){
            throw new CoreException("Table lists must not be null or not be empty.");
        }
        super.javaScripter.invoker(JSFunctionNames.StreamReadFunction.jsName(),context.getConfigContext(),context.getNodeConfig(),offset,tables,readSize,consumer);
    }

    public static StreamReadFunction create(LoadJavaScripter loadJavaScripter) {
        return new JSStreamReadFunction().function(loadJavaScripter);
    }
}
