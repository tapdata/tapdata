package io.tapdata.pdk.core.aspect;

import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptor;
import io.tapdata.entity.aspect.annotations.AspectInterceptorClass;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AspectInterceptorAnnotationHandler extends ClassAnnotationHandler {
    private static final String TAG = AspectInterceptorAnnotationHandler.class.getSimpleName();
    private Map<Class<? extends Aspect>, Collection<AspectInterceptorClassHolder>> aspectInterceptorsMap = new ConcurrentHashMap<>();
    private Map<Class<? extends Aspect>, Collection<AspectInterceptorClassHolder>> newAspectInterceptorsMap = new ConcurrentHashMap<>();

    @Override
    public void handle(Set<Class<?>> classes) throws CoreException {
        if(classes != null) {
            newAspectInterceptorsMap = new ConcurrentHashMap<>();
            TapLogger.debug(TAG, "--------------AspectInterceptor Classes Start-------------");
            for(Class<?> clazz : classes) {
                AspectInterceptorClass aspectInterceptorClass = clazz.getAnnotation(AspectInterceptorClass.class);
                if(aspectInterceptorClass != null) {
                    if(!AspectInterceptor.class.isAssignableFrom(clazz)) {
                        TapLogger.error(TAG, "AspectInterceptor {} don't implement {}, will be ignored", clazz, AspectInterceptor.class);
                        continue;
                    }
                    Class<? extends AspectInterceptor<? extends Aspect>> interceptorClass = (Class<? extends AspectInterceptor<? extends Aspect>>) clazz;
                    Class<? extends Aspect> aspectClass = aspectInterceptorClass.value();
                    int order = aspectInterceptorClass.order();
                    boolean ignoreErrors = aspectInterceptorClass.ignoreErrors();

                    //Check class can be initialized for non-args constructor
                    String canNotInitialized = null;
                    try {
                        Constructor<?> constructor = interceptorClass.getConstructor();
                        if (!Modifier.isPublic(constructor.getModifiers())) {
                            canNotInitialized = "Constructor is not public";
                        }
                    } catch (Throwable e) {
                        canNotInitialized = e.getMessage();
                    }
                    if(canNotInitialized != null) {
                        TapLogger.error(TAG, "AspectInterceptor {} don't have non-args public constructor, will be ignored, message {}", clazz, canNotInitialized);
                        continue;
                    }

                    Collection<AspectInterceptorClassHolder> aspectInterceptors = newAspectInterceptorsMap.get(aspectClass);
                    if(aspectInterceptors == null) {
                        aspectInterceptors = Collections.synchronizedSortedSet(new TreeSet<>());
                        aspectInterceptors.add(new AspectInterceptorClassHolder().aspectClass(interceptorClass).order(order).ignoreErrors(ignoreErrors));
                        newAspectInterceptorsMap.put(aspectClass, aspectInterceptors);
                        TapLogger.debug(TAG, "(New array) AspectInterceptor {} for Aspect {} order {} will be applied", interceptorClass, aspectClass, order);
                    } else {
                        aspectInterceptors.add(new AspectInterceptorClassHolder().aspectClass(interceptorClass).order(order));
                        TapLogger.debug(TAG, "(Exist array) AspectInterceptor {} for Aspect {} order {} will be applied", clazz, aspectClass, order);
                    }
                }
            }
            TapLogger.debug(TAG, "--------------AspectInterceptor Classes End-------------");
        }
        apply();
    }

    public void apply() {
        if(newAspectInterceptorsMap != null) {
            aspectInterceptorsMap = newAspectInterceptorsMap;
            newAspectInterceptorsMap = null;
        }
    }

    @Override
    public Class<? extends Annotation> watchAnnotation() {
        return AspectInterceptorClass.class;
    }

    public Map<Class<? extends Aspect>, Collection<AspectInterceptorClassHolder>> getAspectInterceptorsMap() {
        return aspectInterceptorsMap;
    }
}
