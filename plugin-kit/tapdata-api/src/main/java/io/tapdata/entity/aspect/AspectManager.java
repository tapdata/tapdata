package io.tapdata.entity.aspect;

public interface AspectManager {
    <T extends AspectObserver<? extends Aspect>> T getAspectObserver(Class<T> observerClass);

    <T extends AspectInterceptor<? extends Aspect>> T getAspectInterceptor(Class<T> interceptorClass);

    void executeAspectObservers(Aspect aspect);

    AspectInterceptResult executeAspectInterceptors(Aspect aspect);


}
