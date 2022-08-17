package io.tapdata.entity.aspect;

import java.util.concurrent.Callable;

public interface AspectManager {
    <T extends Aspect> void registerAspectInterceptor(Class<T> aspectClass, int order, AspectInterceptor<T> aspectInterceptor);

    @SuppressWarnings("unchecked")
    <T extends Aspect> void registerAspectInterceptor(Class<T> aspectClass, int order, AspectInterceptor<T> aspectInterceptor, boolean ignoreErrors);

    @SuppressWarnings("unchecked")
    void registerInterceptor(Class<? extends Aspect> aspectClass, int order, AspectInterceptor<Aspect> aspectInterceptor);

    @SuppressWarnings("unchecked")
    void registerInterceptor(Class<? extends Aspect> aspectClass, int order, AspectInterceptor<Aspect> aspectInterceptor, boolean ignoreErrors);

    <T extends Aspect> void unregisterAspectInterceptor(Class<T> aspectClass, Class<? extends AspectInterceptor<T>> observerClass);

    <T extends Aspect> void unregisterAspectInterceptor(Class<T> aspectClass, AspectInterceptor<T> aspectInterceptor);

    void unregisterInterceptor(Class<? extends Aspect> aspectClass, AspectInterceptor<Aspect> aspectInterceptor);

    void registerObserver(Class<? extends Aspect> aspectClass, int order, AspectObserver<Aspect> aspectObserver);

    void registerObserver(Class<? extends Aspect> aspectClass, int order, AspectObserver<Aspect> aspectObserver, boolean ignoreErrors);

    <T extends Aspect> void registerAspectObserver(Class<T> aspectClass, int order, AspectObserver<T> aspectObserver);


    @SuppressWarnings("unchecked")
    <T extends Aspect> void registerAspectObserver(Class<T> aspectClass, int order, AspectObserver<T> aspectObserver, boolean ignoreErrors);

    <T extends Aspect> void unregisterAspectObserver(Class<T> aspectClass, Class<? extends AspectObserver<T>> observerClass);

    <T extends Aspect> void unregisterAspectObserver(Class<T> aspectClass, AspectObserver<T> aspectObserver);

    void unregisterObserver(Class<? extends Aspect> aspectClass, AspectObserver<Aspect> aspectObserver);

    <T extends AspectObserver<?>> T getAspectObserver(Class<T> observerClass);

    <T extends AspectInterceptor<?>> T getAspectInterceptor(Class<T> interceptorClass);

    <T extends Aspect> void executeAspectObservers(Class<T> aspectClass, Callable<T> aspectCallable);

    void executeAspectObservers(Aspect aspect);

    <T extends Aspect> AspectInterceptResult executeAspectInterceptors(Class<T> aspectClass, Callable<T> aspectCallable);

    AspectInterceptResult executeAspectInterceptors(Aspect aspect);


    <T extends Aspect> boolean hasInterceptorOrObserver(Class<T> aspectClass);

    /**
     * Execute Aspect for both interceptors and observers.
     * This method is recommended, best efficiency
     *
     * @param aspectClass Aspect class.
     * @param aspectCallable only create the Aspect when there is any interceptor or observer exists.
     * @return
     * @param <T>
     */
    <T extends Aspect> AspectInterceptResult executeAspect(Class<T> aspectClass, Callable<T> aspectCallable);

    /**
     * Execute Aspect for both interceptors and observers.
     *
     * @param aspect
     * @return
     * @param <T>
     */
    <T extends Aspect> AspectInterceptResult executeAspect(T aspect);
}
