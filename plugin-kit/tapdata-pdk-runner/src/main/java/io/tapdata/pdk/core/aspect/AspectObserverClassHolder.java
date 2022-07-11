package io.tapdata.pdk.core.aspect;

import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptor;
import io.tapdata.entity.aspect.AspectObserver;

public class AspectObserverClassHolder implements Comparable<AspectObserverClassHolder> {
    private Class<? extends AspectObserver<? extends Aspect>> aspectClass;
    public AspectObserverClassHolder aspectClass(Class<? extends AspectObserver<? extends Aspect>> aspectClass) {
        this.aspectClass = aspectClass;
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

    @Override
    public int compareTo(AspectObserverClassHolder observerClassHolder) {
//        if(order == interceptorClassHolder.order)
//            return 0;
        return order > observerClassHolder.order ? 1 : -1;
    }
}
