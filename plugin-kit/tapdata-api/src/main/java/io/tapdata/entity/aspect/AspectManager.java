package io.tapdata.entity.aspect;

import java.util.concurrent.Callable;

public interface AspectManager {
    <T extends Aspect> void registerAspectInterceptor(Class<T> aspectClass, int order, AspectInterceptor<T> aspectInterceptor);

    <T extends Aspect> void unregisterAspectInterceptor(Class<T> aspectClass, Class<? extends AspectInterceptor<T>> observerClass);

    <T extends Aspect> void unregisterAspectInterceptor(Class<T> aspectClass, AspectInterceptor<T> aspectInterceptor);

    <T extends Aspect> void registerAspectObserver(Class<T> aspectClass, int order, AspectObserver<T> aspectObserver);


    <T extends Aspect> void unregisterAspectObserver(Class<T> aspectClass, Class<? extends AspectObserver<T>> observerClass);

    <T extends Aspect> void unregisterAspectObserver(Class<T> aspectClass, AspectObserver<T> aspectObserver);

    <T extends AspectObserver<?>> T getAspectObserver(Class<T> observerClass);

    <T extends AspectInterceptor<?>> T getAspectInterceptor(Class<T> interceptorClass);

    <T extends Aspect> void executeAspectObservers(Class<T> aspectClass, Callable<T> aspectCallable);

    void executeAspectObservers(Aspect aspect);

    <T extends Aspect> AspectInterceptResult executeAspectInterceptors(Class<T> aspectClass, Callable<T> aspectCallable);

    AspectInterceptResult executeAspectInterceptors(Aspect aspect);


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
