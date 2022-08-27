package io.tapdata.pdk.core.implementation;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;
import io.tapdata.entity.utils.InstanceFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BeanAnnotationHandler extends ClassAnnotationHandler {
    private static final String TAG = BeanAnnotationHandler.class.getSimpleName();

    @Override
    public void handle(Set<Class<?>> classes) throws CoreException {
        if(classes != null) {
            TapLogger.debug(TAG, "--------------Implementation Classes Start-------------");
            for(Class<?> clazz : classes) {
                Bean implementation = clazz.getAnnotation(Bean.class);
                if(implementation != null) {
                    //Check class can be initialized for non-args constructor
                    String canNotInitialized = null;
                    try {
                        Constructor<?> constructor = clazz.getConstructor();
                        if (!Modifier.isPublic(constructor.getModifiers())) {
                            canNotInitialized = "Constructor is not public";
                        }
                    } catch (Throwable e) {
                        canNotInitialized = e.getMessage();
                    }
                    if(canNotInitialized != null) {
                        TapLogger.warn(TAG, "Bean {} don't have non-args public constructor, will be ignored, message {}", clazz, canNotInitialized);
                        continue;
                    }

                    InstanceFactory.bean(clazz);
                }
            }
            TapLogger.debug(TAG, "--------------Implementation Classes End-------------");
            InstanceFactory.injectBeans();
            TapLogger.debug(TAG, "--------------Beans are all injected-------------");
        }
    }

    @Override
    public Class<? extends Annotation> watchAnnotation() {
        return Bean.class;
    }

}
