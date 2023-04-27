package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.js.connector.JSConnector;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;

import java.util.Objects;

public class JSTimestampToStreamOffsetFunction extends FunctionBase implements FunctionSupport<TimestampToStreamOffsetFunction> {
    private static final String TAG = JSTimestampToStreamOffsetFunction.class.getSimpleName();

    private JSTimestampToStreamOffsetFunction() {
        super();
        super.functionName = JSFunctionNames.TimestampToStreamOffset;
    }

    @Override
    public TimestampToStreamOffsetFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) return this::defaultTimestampToStreamOffset;
        return this::timestampToStreamOffset;
    }

    private Object defaultTimestampToStreamOffset(TapConnectorContext tapConnectorContext, Long time) {
        return time != null ? time : System.currentTimeMillis();
    }

    private Object timestampToStreamOffset(TapConnectorContext context, Long time) {
        if (Objects.isNull(context)) {
            throw new CoreException("TapConnectorContext cannot be empty.");
        }
        Object invoker;
        synchronized (JSConnector.execLock) {
            invoker = super.javaScripter.invoker(
                    JSFunctionNames.TimestampToStreamOffset.jsName(),
                    time
            );
        }
        if (Objects.isNull(invoker)) {
            TapLogger.info(TAG, "JavaScript execution result is empty, The current timestamp is returned.");
            return System.currentTimeMillis();
        }
        return invoker;
    }

    public static TimestampToStreamOffsetFunction create(LoadJavaScripter loadJavaScripter) {
        return new JSTimestampToStreamOffsetFunction().function(loadJavaScripter);
    }
}
