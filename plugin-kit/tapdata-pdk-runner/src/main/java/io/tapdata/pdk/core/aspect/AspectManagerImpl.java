package io.tapdata.pdk.core.aspect;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.aspect.*;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationManager;
import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Implementation(AspectManager.class)
public class AspectManagerImpl implements AspectManager {
    private static final String TAG = AspectManagerImpl.class.getSimpleName();
    private final AspectInterceptorAnnotationHandler aspectInterceptorAnnotationHandler;
    private final AspectObserverAnnotationHandler aspectObserverAnnotationHandler;
    private Map<Class<? extends Aspect>, Collection<AspectInterceptorClassHolder>> aspectInterceptorMap;
    private Map<Class<? extends Aspect>, Collection<AspectObserverClassHolder>> aspectObserversMap;
    private final Map<Class<? extends AspectObserver<? extends Aspect>>, AspectObserver<? extends Aspect>> aspectObserverInstanceMap = new ConcurrentHashMap<>();
    private final Map<Class<? extends AspectInterceptor<? extends Aspect>>, AspectInterceptor<? extends Aspect>> aspectInterceptorInstanceMap = new ConcurrentHashMap<>();
    public AspectManagerImpl() {
        aspectInterceptorAnnotationHandler = new AspectInterceptorAnnotationHandler();
        aspectObserverAnnotationHandler = new AspectObserverAnnotationHandler();

        ClassAnnotationManager classAnnotationManager = ClassFactory.create(ClassAnnotationManager.class);
        if(classAnnotationManager != null) {
            classAnnotationManager
                    .registerClassAnnotationHandler(aspectInterceptorAnnotationHandler)
                    .registerClassAnnotationHandler(aspectObserverAnnotationHandler);
            String scanPackage = CommonUtils.getProperty("pdk_aspect_scan_package", "io,tapdata");
            String[] packages = scanPackage.split(",");
            classAnnotationManager.scan(packages, this.getClass().getClassLoader());

            aspectInterceptorMap = aspectInterceptorAnnotationHandler.getAspectInterceptorsMap();
            aspectObserversMap = aspectObserverAnnotationHandler.getAspectObserversMap();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends AspectObserver<? extends Aspect>> T getAspectObserver(Class<T> observerClass) {
        return (T) aspectObserverInstanceMap.get(observerClass);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends AspectInterceptor<? extends Aspect>> T getAspectInterceptor(Class<T> interceptorClass) {
        return (T) aspectInterceptorInstanceMap.get(interceptorClass);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void executeAspectObservers(Aspect aspect) {
        if(aspectObserversMap != null && aspect != null) {
            Collection<AspectObserverClassHolder> observerClasses = aspectObserversMap.get(aspect.getClass());
            if(observerClasses != null) {
                for(AspectObserverClassHolder observerClass : observerClasses) {
                    AspectObserver observer = aspectObserverInstanceMap.computeIfAbsent(observerClass.getAspectClass(), aClass -> {
                        try {
                            return observerClass.getAspectClass().getConstructor().newInstance();
                        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                                 NoSuchMethodException e) {
                            throw new RuntimeException("Instantiate AspectObserver sub class " + observerClass + " failed, " + e.getMessage());
                        }
                    });
                    CommonUtils.ignoreAnyError(() -> {
                        observer.observe(aspect);
                    }, TAG);
                }
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public AspectInterceptResult executeAspectInterceptors(Aspect aspect) {
        if(aspectInterceptorMap != null && aspect != null) {
            Collection<AspectInterceptorClassHolder> observerClasses = aspectInterceptorMap.get(aspect.getClass());
            if(observerClasses != null) {
                AtomicReference<AspectInterceptResult> resultAtomicReference = new AtomicReference<>();
                for(AspectInterceptorClassHolder interceptClass : observerClasses) {
                    AspectInterceptor interceptor = aspectInterceptorInstanceMap.computeIfAbsent(interceptClass.getAspectClass(), aClass -> {
                        try {
                            return interceptClass.getAspectClass().getConstructor().newInstance();
                        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                                 NoSuchMethodException e) {
                            throw new RuntimeException("Instantiate AspectInterceptor sub class " + interceptClass + " failed, " + e.getMessage());
                        }
                    });
                    CommonUtils.ignoreAnyError(() -> {
                        resultAtomicReference.set(interceptor.intercept(aspect));
                    }, TAG);
                    if(resultAtomicReference.get() != null && resultAtomicReference.get().isIntercepted()) {
                        TapLogger.info(TAG, "Aspect {} intercepted {}", aspect, resultAtomicReference.get());
                        break;
                    }
                }
                return resultAtomicReference.get();
            }
        }
        return null;
    }



}
