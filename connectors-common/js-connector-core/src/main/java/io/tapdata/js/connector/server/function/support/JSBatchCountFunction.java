package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.js.connector.JSConnector;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;

import java.util.Objects;


public class JSBatchCountFunction extends FunctionBase implements FunctionSupport<BatchCountFunction> {
    private static final String TAG = JSBatchCountFunction.class.getSimpleName();

    private JSBatchCountFunction() {
        super();
        super.functionName = JSFunctionNames.BatchCountFunction;
    }

    @Override
    public BatchCountFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) return null;
        return this::batchCount;
    }

    private long batchCount(TapConnectorContext context, TapTable table) {
        if (Objects.isNull(context)) {
            throw new CoreException("TapConnectorContext cannot not be empty.");
        }
        if (Objects.isNull(table)) {
            throw new CoreException("Table lists cannot not be empty.");
        }
        Object invoker;
        synchronized (JSConnector.execLock) {
            invoker = super.javaScripter.invoker(
                    JSFunctionNames.BatchCountFunction.jsName(),
                    context.getConfigContext(),
                    context.getNodeConfig(),
                    table.getId()
            );
        }
        if (Objects.isNull(invoker)) {
            TapLogger.info(TAG, "JavaScript execution result cannot be NULL or empty, please return Long type result.");
            return 0L;
        }
        try {
            return Long.parseLong(String.valueOf(invoker));
        } catch (Exception e) {
            TapLogger.warn(TAG, String.format("JavaScript execution result cannot be converted to Long, please return Long type result.Msg: %s. ", e.getMessage()));
            return 0;
        }
    }


    public static BatchCountFunction create(LoadJavaScripter loadJavaScripter) {
        return new JSBatchCountFunction().function(loadJavaScripter);
    }
}
