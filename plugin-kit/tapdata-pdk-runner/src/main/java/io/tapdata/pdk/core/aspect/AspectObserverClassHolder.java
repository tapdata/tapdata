package io.tapdata.pdk.core.aspect;

import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptor;
import io.tapdata.entity.aspect.AspectObserver;

public class AspectObserverClassHolder implements Comparable<AspectObserverClassHolder> {
    private AspectObserver<? extends Aspect> aspectObserver;
    public AspectObserverClassHolder aspectObserver(AspectObserver<? extends Aspect> aspectObserver) {
        this.aspectObserver = aspectObserver;
        return this;
    }

    private Class<? extends AspectObserver<? extends Aspect>> aspectClass;
    public AspectObserverClassHolder aspectClass(Class<? extends AspectObserver<? extends Aspect>> aspectClass) {
        this.aspectClass = aspectClass;
        return this;
    }
    private boolean ignoreErrors = true;
    public AspectObserverClassHolder ignoreErrors(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
        return this;
    }
    private int order;
    public AspectObserverClassHolder order(int order) {
        this.order = order;
        return this;
    }

    public Class<? extends AspectObserver<? extends Aspect>> getAspectClass() {
        return aspectClass;
    }

    public void setAspectClass(Class<? extends AspectObserver<? extends Aspect>> aspectClass) {
        this.aspectClass = aspectClass;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public AspectObserver<? extends Aspect> getAspectObserver() {
        return aspectObserver;
    }

    public void setAspectObserver(AspectObserver<? extends Aspect> aspectObserver) {
        this.aspectObserver = aspectObserver;
    }

    @Override
    public int compareTo(AspectObserverClassHolder observerClassHolder) {
//        if(order == interceptorClassHolder.order)
//            return 0;
        return order > observerClassHolder.order ? 1 : -1;
    }

    public boolean isIgnoreErrors() {
        return ignoreErrors;
    }

    public void setIgnoreErrors(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
    }
}
