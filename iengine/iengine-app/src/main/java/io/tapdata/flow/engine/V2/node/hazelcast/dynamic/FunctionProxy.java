package io.tapdata.flow.engine.V2.node.hazelcast.dynamic;

import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public abstract class FunctionProxy<T extends TapFunction> {
    T function;

    protected FunctionProxy(T function) {
        this.function = function;
    }

    public T getFunction() {
        return function;
    }

    protected boolean doBefore(Object[] args) {
        return true;
    }

    protected Object doing(T original, Object proxyObj, Method method, Object[] args) throws InvocationTargetException, IllegalAccessException {
        return method.invoke(original, args);
    }

    protected void doAfter(Object[] args) {
        //do nothing now
    }

    public static String functionName(Object function) {
        if (function instanceof FunctionProxy<?> proxy) {
            return proxy.getFunction().getClass().getSimpleName();
        }
        return function.getClass().getSimpleName();
    }
}
