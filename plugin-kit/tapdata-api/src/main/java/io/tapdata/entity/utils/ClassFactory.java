package io.tapdata.entity.utils;

import java.lang.reflect.Method;

public class ClassFactory {
    private static volatile Object classFactory;
    private static Method createMethod;
    private static Method createMethod2;
    private static final Object lock = new int[0];
    private ClassFactory() {}

    public static <T> T create(Class<T> clazz) {
        initClassFactory();
        try {
            return (T) createMethod.invoke(classFactory, clazz);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }

    public static <T> T create(Class<T> clazz, String type) {
        initClassFactory();
        try {
            return (T) createMethod2.invoke(classFactory, clazz, type);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void initClassFactory() {
        if(classFactory == null) {
            synchronized (ClassFactory.class) {
                if(classFactory == null) {
                    String classFactoryStr = "io.tapdata.pdk.core.runtime.TapRuntime";
                    try {
                        Class<?> clazz1 = Class.forName(classFactoryStr);
                        Method method = clazz1.getDeclaredMethod("getInstance");
                        Object tapRuntime = method.invoke(null);
                        Method factoryMethod = clazz1.getDeclaredMethod("getImplementationClassFactory");
                        classFactory = factoryMethod.invoke(tapRuntime);
                        createMethod = classFactory.getClass().getDeclaredMethod("create", Class.class);
                        createMethod2 = classFactory.getClass().getDeclaredMethod("create", Class.class, String.class);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

}
