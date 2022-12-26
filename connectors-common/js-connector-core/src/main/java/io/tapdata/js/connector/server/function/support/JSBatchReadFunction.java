package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;

import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

public class JSBatchReadFunction extends FunctionBase implements FunctionSupport<BatchReadFunction> {
    private JSBatchReadFunction(){
        super();
        super.functionName = JSFunctionNames.BatchReadFunction;
    }

    @Override
    public BatchReadFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) return null;
        return this::batchRead;
    }

    private void batchRead(TapConnectorContext context, TapTable table, Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        if (Objects.isNull(context)){
            throw new CoreException("TapConnectorContext must not be null or not be empty.");
        }
        if(Objects.isNull(table)){
            throw new CoreException("TapTable must not be null or not be empty.");
        }
        super.javaScripter.invoker(JSFunctionNames.BatchReadFunction.jsName(),context.getConfigContext(),context.getNodeConfig(),offset,table.getId(),batchCount,consumer);
    }

    public static BatchReadFunction create(LoadJavaScripter loadJavaScripter) {
        return new JSBatchReadFunction().function(loadJavaScripter);
    }
}
