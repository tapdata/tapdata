package io.tapdata.flow.engine.V2.node.hazelcast.dynamic;

import io.tapdata.pdk.apis.functions.connector.TapFunction;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;

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

    protected T cglibProxy() {
        T original = this.getFunction();
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(original.getClass());
        enhancer.setCallback((MethodInterceptor) (obj, method, args, proxy) -> {
            doBefore(args);
            try {
                return proxy.invoke(original, args);
            } finally {
                doAfter(args);
            }
        });
        return (T) enhancer.create();
    }

    public T proxy() {
        if (null == getFunction()) {
            return null;
        }
        return cglibProxy();
    }
}
