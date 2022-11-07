package io.tapdata.pdk.core.implementation;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.annotations.MainMethod;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;
import io.tapdata.entity.utils.Container;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ReflectionUtil;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class BeanAnnotationHandler extends ClassAnnotationHandler {
    private static final String TAG = BeanAnnotationHandler.class.getSimpleName();
    private SortedSet<MainMethodWrapper> mainMethodList = Collections.synchronizedSortedSet(new TreeSet<>());
    static class MainMethodWrapper implements Comparable<MainMethodWrapper> {
        private String method;
        private Class<?> theClass;
        private int order;
        public MainMethodWrapper(String method, Class<?> theClass, int order) {
            this.method = method;
            this.theClass = theClass;
            this.order = order;
        }

        @Override
        public int compareTo(MainMethodWrapper sessionClassHolder) {
//        if(order == interceptorClassHolder.order)
//            return 0;
            return order > sessionClassHolder.order ? 1 : -1;
        }
    }
    @Override
    public void handle(Set<Class<?>> classes) throws CoreException {
        if(classes != null) {
            TapLogger.debug(TAG, "--------------Implementation Classes Start-------------");
            for(Class<?> clazz : classes) {
                Bean bean = clazz.getAnnotation(Bean.class);
                if(bean != null) {
                    MainMethod mainMethod = clazz.getAnnotation(MainMethod.class);
                    if(mainMethod != null) {
                        String mainMethodStr = mainMethod.value();
                        int order = mainMethod.order();
                        if(mainMethodStr.length() > 0) {
                            mainMethodList.add(new MainMethodWrapper(mainMethodStr, clazz, order));
                        }
                    }
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
                    TapLogger.debug(TAG, "Found implementation class {}", clazz);
                    long time = System.currentTimeMillis();
                    InstanceFactory.bean(clazz);
                    TapLogger.debug(TAG, "Bean implementation class {} takes {}", clazz, (System.currentTimeMillis() - time));
                }
            }
            TapLogger.debug(TAG, "--------------Implementation Classes End-------------");
            InstanceFactory.injectBeans();
            TapLogger.debug(TAG, "--------------Beans are all injected-------------");
            for(MainMethodWrapper mainMethodWrapper : mainMethodList) {
                Object beanObj = InstanceFactory.bean(mainMethodWrapper.theClass);
                if(beanObj != null) {
                    try {
//                        Method method = beanObj.getClass().getMethod(mainMethodWrapper.method);
                        Method method = ReflectionUtil.getDeclaredMethod(beanObj.getClass(), mainMethodWrapper.method, new Class<?>[0]);
                        if(method != null) {
                            method.setAccessible(true);
                            method.invoke(beanObj);
                        } else {
                            TapLogger.warn(TAG, "Class {} method {} not found, no main method will be invoked", beanObj.getClass(), mainMethodWrapper.method);
                        }
                    } catch (Throwable e) {
                        TapLogger.warn(TAG, "Invoke main method {} in class {} failed, {}", mainMethodWrapper.method, mainMethodWrapper.theClass, e.getMessage());
                    }
                }
            }
        }
    }

    @Override
    public Class<? extends Annotation> watchAnnotation() {
        return Bean.class;
    }

}
