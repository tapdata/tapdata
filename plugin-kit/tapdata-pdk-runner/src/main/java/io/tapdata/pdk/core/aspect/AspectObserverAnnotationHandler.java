package io.tapdata.pdk.core.aspect;

import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AspectObserverAnnotationHandler extends ClassAnnotationHandler {
    private static final String TAG = AspectObserverAnnotationHandler.class.getSimpleName();
    private Map<Class<? extends Aspect>, Collection<AspectObserverClassHolder>> aspectObserversMap = new ConcurrentHashMap<>();
    private Map<Class<? extends Aspect>, Collection<AspectObserverClassHolder>> newAspectObserversMap = new ConcurrentHashMap<>();

    @Override
    public void handle(Set<Class<?>> classes) throws CoreException {
        if(classes != null) {
            newAspectObserversMap = new ConcurrentHashMap<>();
            TapLogger.debug(TAG, "--------------AspectObserver Classes Start-------------");
            for(Class<?> clazz : classes) {
                AspectObserverClass aspectObserverClass = clazz.getAnnotation(AspectObserverClass.class);
                if(aspectObserverClass != null) {
                    if(!AspectObserver.class.isAssignableFrom(clazz)) {
                        TapLogger.error(TAG, "AspectObserver {} don't implement {}, will be ignored", clazz, AspectObserver.class);
                        continue;
                    }
                    Class<? extends AspectObserver<? extends Aspect>> observerClass = (Class<? extends AspectObserver<? extends Aspect>>) clazz;
                    Class<? extends Aspect> aspectClass = aspectObserverClass.value();
                    int order = aspectObserverClass.order();
                    boolean ignoreErrors = aspectObserverClass.ignoreErrors();

                    //Check class can be initialized for non-args constructor
                    String canNotInitialized = null;
                    try {
                        Constructor<?> constructor = observerClass.getConstructor();
                        if (!Modifier.isPublic(constructor.getModifiers())) {
                            canNotInitialized = "Constructor is not public";
                        }
                    } catch (Throwable e) {
                        canNotInitialized = e.getMessage();
                    }
                    if(canNotInitialized != null) {
                        TapLogger.error(TAG, "AspectObserver {} don't have non-args public constructor, will be ignored, message {}", clazz, canNotInitialized);
                        continue;
                    }

                    Collection<AspectObserverClassHolder> implClasses = newAspectObserversMap.get(aspectClass);
                    if(implClasses == null) {
                        implClasses = Collections.synchronizedSortedSet(new TreeSet<>());
                        implClasses.add(new AspectObserverClassHolder().aspectClass(observerClass).order(order).ignoreErrors(ignoreErrors));
                        newAspectObserversMap.put(aspectClass, implClasses);
                        TapLogger.debug(TAG, "(New array) AspectObserver {} for Aspect {} will be applied", observerClass, aspectClass);
                    } else {
                        implClasses.add(new AspectObserverClassHolder().aspectClass(observerClass).order(order).ignoreErrors(ignoreErrors));
                        TapLogger.debug(TAG, "(Exist array) AspectObserver {} for Aspect {} will be applied", clazz, aspectClass);
                    }
                }
            }
            TapLogger.debug(TAG, "--------------AspectObserver Classes End-------------");
        }
        apply();
    }

    public void apply() {
        if(newAspectObserversMap != null) {
            aspectObserversMap = newAspectObserversMap;
            newAspectObserversMap = null;
        }
    }

    public Map<Class<? extends Aspect>, Collection<AspectObserverClassHolder>> getAspectObserversMap() {
        return aspectObserversMap;
    }

    @Override
    public Class<? extends Annotation> watchAnnotation() {
        return AspectObserverClass.class;
    }
}
