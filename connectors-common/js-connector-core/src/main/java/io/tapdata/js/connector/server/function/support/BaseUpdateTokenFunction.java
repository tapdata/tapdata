package io.tapdata.js.connector.server.function.support;

import io.tapdata.common.support.entitys.APIResponse;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.Map;
import java.util.Objects;

public class BaseUpdateTokenFunction extends FunctionBase implements FunctionSupport<BaseUpdateTokenFunction> {
    private static final String TAG = BaseUpdateTokenFunction.class.getSimpleName();
    private BaseUpdateTokenFunction() {
        super();
        super.functionName = JSFunctionNames.UPDATE_TOKEN;
    }
    private TapConnectionContext connectionContext;
    public BaseUpdateTokenFunction connectionContext(TapConnectionContext connectionContext){
        this.connectionContext = connectionContext;
        return this;
    }
    @Override
    public BaseUpdateTokenFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) return null;
        return this;
    }

    public Map<String,Object> exec(APIResponse response){
        if (Objects.isNull(this.connectionContext)) {
            throw new CoreException("TapConnectorContext cannot not be empty.");
        }
        Object invoker = super.javaScripter.invoker(
                JSFunctionNames.BatchCountFunction.jsName(),
                this.connectionContext.getConnectionConfig(),
                this.connectionContext.getNodeConfig(),
                response
        );
        try {
            return Objects.isNull(invoker)? null : (Map<String, Object>) invoker;
        } catch (Exception e) {
            TapLogger.warn(TAG, String.format("JavaScript execution result cannot be converted to Map, please return Map type result.Msg: %s. ", e.getMessage()));
            return null;
        }
    }

    public static BaseUpdateTokenFunction create(LoadJavaScripter loadJavaScripter, TapConnectionContext connectionContext) {
        return new BaseUpdateTokenFunction().function(loadJavaScripter).connectionContext(connectionContext);
    }
}
