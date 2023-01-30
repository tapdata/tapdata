package io.tapdata.js.connector.server.function;

import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface FunctionSupport<T> extends TapFunction {
    public static <T> T function(LoadJavaScripter javaScripter, FunctionSupport<T> functionSupport) {
        return functionSupport.function(javaScripter);
    }

    public T function(LoadJavaScripter javaScripter);
}
