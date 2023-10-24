package io.tapdata.entity.utils;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.logger.TapLogger;

import java.lang.reflect.Method;

public class ClassFactory {
    private static final String TAG = ClassFactory.class.getSimpleName();
    private static volatile Object classFactory;
    private static Method createMethod;
    private static Method createMethod2;
    private static Method getImplClass;
    private ClassFactory() {}

    public static <T> T create(Class<T> clazz) {
        initClassFactory();
        try {
            //noinspection unchecked
            return (T) createMethod.invoke(classFactory, clazz);
        } catch (Throwable e) {
            throw new CoreException(TapAPIErrorCodes.ERROR_CREATE_CLASS_FAILED, "create for class {}, failed {}", clazz, e.getMessage());
        }
    }

    public static <T> T create(Class<T> clazz, String type) {
        initClassFactory();
        try {
            //noinspection unchecked
            return (T) createMethod2.invoke(classFactory, clazz, type);
        } catch (Throwable e) {
            throw new CoreException(TapAPIErrorCodes.ERROR_CREATE_CLASS_WITH_TYPE_FAILED, "create for class {} type {}, failed {}", clazz, type, e.getMessage());
        }
    }

    public static <T> Class<T> getImplementationClass(Class<T> tClass, String type) {
        try {
            //noinspection unchecked
            return (Class<T>) getImplClass.invoke(classFactory, tClass, type);
        } catch (Throwable e) {
            throw new CoreException(TapAPIErrorCodes.ERROR_GET_IMPL_CLASS_FAILED, "getImplementationClass for class {} type {}, failed {}", tClass, type, e.getMessage());
        }
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
                        getImplClass = classFactory.getClass().getDeclaredMethod("getImplementationClass", Class.class, String.class);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        TapLogger.error(TAG, "Initiate {} failed, {}", classFactoryStr, e.getMessage());
                    }
                }
            }
        }
    }

}
