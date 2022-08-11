package io.tapdata.pdk.core.aspect;

import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptor;

import java.util.*;

public class AspectInterceptorClassHolder implements Comparable<AspectInterceptorClassHolder>{
    private AspectInterceptor<? extends Aspect> aspectInterceptor;
    public AspectInterceptorClassHolder aspectInterceptor(AspectInterceptor<? extends Aspect> aspectInterceptor) {
        this.aspectInterceptor = aspectInterceptor;
        return this;
    }
    private Class<? extends AspectInterceptor<? extends Aspect>> aspectClass;
    public AspectInterceptorClassHolder aspectClass(Class<? extends AspectInterceptor<? extends Aspect>> aspectClass) {
        this.aspectClass = aspectClass;
        return this;
    }

    private boolean ignoreErrors = true;
    public AspectInterceptorClassHolder ignoreErrors(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
        return this;
    }
    private int order;
    public AspectInterceptorClassHolder order(int order) {
        this.order = order;
        return this;
    }

    public Class<? extends AspectInterceptor<? extends Aspect>> getAspectClass() {
        return aspectClass;
    }

    public void setAspectClass(Class<? extends AspectInterceptor<? extends Aspect>> aspectClass) {
        this.aspectClass = aspectClass;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public AspectInterceptor<? extends Aspect> getAspectInterceptor() {
        return aspectInterceptor;
    }

    public void setAspectInterceptor(AspectInterceptor<? extends Aspect> aspectInterceptor) {
        this.aspectInterceptor = aspectInterceptor;
    }

    @Override
    public int compareTo(AspectInterceptorClassHolder interceptorClassHolder) {
//        if(order == interceptorClassHolder.order)
//            return 0;
        return order > interceptorClassHolder.order ? 1 : -1;
    }

    public static void main(String[] args) {
        Collection<AspectInterceptorClassHolder> list = new TreeSet<>();
        list.add(new AspectInterceptorClassHolder().order(3));
        list.add(new AspectInterceptorClassHolder().order(13));
        list.add(new AspectInterceptorClassHolder().order(31));
        list.add(new AspectInterceptorClassHolder().order(13));
        list.add(new AspectInterceptorClassHolder().order(1));

//        Collections.sort(list);
        for (AspectInterceptorClassHolder classHolder : list) {
            System.out.println(classHolder.order);
        }
        List<AspectInterceptorClassHolder> holders = new ArrayList<>(list);
        for (AspectInterceptorClassHolder classHolder : holders) {
            System.out.println("holders " + classHolder.order);
        }
    }

    public boolean isIgnoreErrors() {
        return ignoreErrors;
    }

    public void setIgnoreErrors(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
    }
}
