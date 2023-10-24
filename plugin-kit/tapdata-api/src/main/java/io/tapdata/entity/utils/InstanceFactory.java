package io.tapdata.entity.utils;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.FieldAnnotationHandler;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class InstanceFactory {
    private static final Map<Class<?>, BeanWrapper> beanMap = new ConcurrentHashMap<>();
    private static final Map<Class<?>, Object> instanceMap = new ConcurrentHashMap<>();
    private static final Map<String, Object> instanceTypeMap = new ConcurrentHashMap<>();
    private static final String TAG = InstanceFactory.class.getSimpleName();

    private static Function<Class<?>, Object> beanInterceptor;

    private final static List<FieldAnnotationHandler<?>> fieldAnnotationHandlers = new CopyOnWriteArrayList<>();
    private static final AtomicBoolean initializing = new AtomicBoolean(false);
    private static final AtomicBoolean initialized = new AtomicBoolean(false);

    private static class BeanWrapper {
        private final Object object;
        private volatile boolean isInjected;

        public BeanWrapper(Object object, boolean isInjected) {
            this.object = object;
            this.isInjected = isInjected;
        }
    }
    private InstanceFactory() {}

    public static void registerFieldAnnotationHandler(FieldAnnotationHandler<?> fieldAnnotationHandler) {
        if(!fieldAnnotationHandlers.contains(fieldAnnotationHandler)) {
            fieldAnnotationHandlers.add(fieldAnnotationHandler);
        }
    }

    /**
     * Use this way to integration with Spring beans.
     * @param beanInterceptor
     */
    public static void setBeanInterceptor(Function<Class<?>, Object> beanInterceptor) {
        InstanceFactory.beanInterceptor = beanInterceptor;
    }

    public static void injectBean(Object beanObject) throws CoreException {
        injectBean(beanObject, false);
    }
    public static void injectBean(Object beanObject, boolean needInjectBeans) throws CoreException {
        Field[] fields = ReflectionUtil.getFields(beanObject.getClass());
        for (Field field : fields) {
            Bean bean = field.getAnnotation(Bean.class);
            if (bean != null) {
                Class<?> gClass = field.getType();
                String type = (bean.type() != null && !bean.type().equals("")) ? bean.type() : null;

                Object beanValue = bean(gClass, needInjectBeans, type);

                if (beanValue != null) {
                    field.setAccessible(true);
                    try {
                        field.set(beanObject, beanValue);
                    } catch (Throwable e) {
                        TapLogger.warn(TAG, "Assign bean field {} value {} to {} failed, {}", field.getName(), beanObject, beanValue, e.getMessage());
                    }
                }
            } else {
                for (FieldAnnotationHandler<?> listener : fieldAnnotationHandlers) {
                    try {
                        Class<? extends Annotation> annotationClass = listener.annotationClass();
                        if (annotationClass != null) {
                            Annotation annotation = field.getAnnotation(annotationClass);
                            if (annotation != null) {
                                Object value = listener.inject(annotation, field);
                                if (value != null) {
                                    field.setAccessible(true);
                                    try {
                                        field.set(beanObject, value);
                                    } catch (Throwable e) {
                                        TapLogger.warn(TAG, "Assign {} field {} value {} to {} failed, {}", annotationClass, field.getName(), beanObject, value, e.getMessage());
                                    }
                                }
                                break;
                            }
                        }
                    } catch (Throwable t) {
                        t.printStackTrace();
                        TapLogger.warn(TAG, "handle field inject listener " + listener + " failed, " + t.getMessage());
                    }
                }
            }
        }
    }

    public static <T> T registerBean(Class<T> beanClass, Object object) {
        injectBean(object);
        beanMap.put(beanClass, new BeanWrapper(object, true));
        //noinspection unchecked
        return (T) object;
    }

    public static <T> T bean(Class<T> beanClass, String type) {
        return bean(beanClass, false, type);
    }
    public static <T> T bean(Class<T> beanClass) {
        return bean(beanClass, false, null);
    }

    public static <T> T bean(Class<T> beanClass, boolean needInject) {
        return bean(beanClass, needInject, null);
    }
    public static <T> T bean(Class<T> beanClass, boolean needInject, String type) {
        BeanWrapper beanWrapper = beanMap.get(beanClass);
        if(beanWrapper != null && beanWrapper.object != null) {
            if(!beanWrapper.isInjected && needInject) {
                synchronized (beanWrapper.object) {
                    if(!beanWrapper.isInjected) {
                        TapLogger.debug(TAG, "inject bean {} as needed", beanWrapper.object);
                        injectBean(beanWrapper.object);
                        TapLogger.debug(TAG, "injected bean {} as needed", beanWrapper.object);
                        beanWrapper.isInjected = true;
                    }
                }
            }
            //noinspection unchecked
            return (T) beanWrapper.object;
        }
        final AtomicReference<Object> beanValue = new AtomicReference<>();
        if(beanInterceptor != null) {
            try {
                beanValue.set(beanInterceptor.apply(beanClass));
            } catch (Throwable throwable) {
                TapLogger.warn(TAG, "beanInterceptor apply failed, beanClass {}, error {}, continue to look for other ways", beanClass, throwable.getMessage());
            }
        }
        if(beanValue.get() == null) {
            Object obj;
            if(type != null) {
                obj = instance(beanClass, type);
            } else {
                obj = instance(beanClass);
            }
            if(obj != null) {
                beanValue.set(obj);
            }
        }
        AtomicBoolean newCreated = new AtomicBoolean(false);

        beanWrapper = beanMap.computeIfAbsent(beanClass, aClass -> {
            try {
                newCreated.set(true);
                if(beanValue.get() != null) {
                    return new BeanWrapper(beanValue.get(), false);
                }
                return new BeanWrapper(beanClass.getConstructor().newInstance(), false);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                     NoSuchMethodException e) {
//                throw new RuntimeException(e);
                TapLogger.error(TAG, "Create class {} failed, {}", beanClass, e.getMessage());
                return null;
            }
        });
        if(beanWrapper == null)
            return null;
        if(newCreated.get() && needInject && !beanWrapper.isInjected) {
            synchronized (beanWrapper.object) {
                if(!beanWrapper.isInjected) {
                    TapLogger.debug(TAG, "inject bean {} for new created", beanWrapper.object);
                    injectBean(beanWrapper.object);
                    TapLogger.debug(TAG, "injected bean {} for new created", beanWrapper.object);
                    beanWrapper.isInjected = true;
                }
            }
        }
        //noinspection unchecked
        return (T) beanWrapper.object;
    }

    public static <T> T instance(Class<T> instanceClass) {
        return instance(instanceClass, false);
    }
    public static <T> T instance(Class<T> instanceClass, boolean waitUntilInitialized) {
        Object obj = instanceMap.get(instanceClass);
        if(obj == null) {
            obj = ClassFactory.create(instanceClass);
            if(obj == null)
                return null;
            injectBeanForInstance(waitUntilInitialized, obj);
            Object old = instanceMap.putIfAbsent(instanceClass, obj);
        }
        //noinspection unchecked
        return (T) obj;

        //noinspection unchecked
//        return (T) instanceMap.computeIfAbsent(instanceClass, aClass -> {
//            Object obj = ClassFactory.create(instanceClass);
//            if(obj != null && initialized.get())
//                injectBean(obj);
//            return obj;
//        });
    }

    private static void injectBeanForInstance(boolean waitUntilInitialized, Object obj) {
        if(waitUntilInitialized) {
            while (!initialized.get()) {
                synchronized (initialized) {
                    try {
                        initialized.wait(100L);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        if(initialized.get()) {
            injectBean(obj);
        }
    }

    public static <T> T instance(Class<T> instanceClass, String type) {
        return instance(instanceClass, type, false);
    }
    public static <T> T instance(Class<T> instanceClass, String type, boolean waitUntilInitialized) {
        String key = instanceClass.getName() + "#" + type;
        Object obj = instanceTypeMap.get(key);
        if(obj == null) {
            obj = ClassFactory.create(instanceClass, type);
            if(obj == null)
                return null;
            injectBeanForInstance(waitUntilInitialized, obj);
            Object old = instanceTypeMap.putIfAbsent(key, obj);
        }
        //noinspection unchecked
        return (T) obj;
        //noinspection unchecked
//        return (T) instanceTypeMap.computeIfAbsent(key, theKey -> ClassFactory.create(instanceClass, type));
    }

    public static void injectBeans() {
        if(initializing.compareAndSet(false, true)) {
            for(Class<?> beanClass : beanMap.keySet()) {
                TapLogger.debug(TAG, "inject bean {}", beanClass);
                long time = System.currentTimeMillis();
                bean(beanClass, true);
                TapLogger.debug(TAG, "injected bean {} takes ", beanClass, (System.currentTimeMillis() - time));
            }
            for(Object instanceObj : instanceMap.values()) {
                TapLogger.debug(TAG, "inject instanceObj {}", instanceObj);
                long time = System.currentTimeMillis();
                injectBean(instanceObj, true);
                TapLogger.debug(TAG, "injected instanceObj {} takes ", instanceObj, (System.currentTimeMillis() - time));
            }
            for(Object typeInstanceObj : instanceTypeMap.values()) {
                TapLogger.debug(TAG, "inject typeInstanceObj {}", typeInstanceObj);
                long time = System.currentTimeMillis();
                injectBean(typeInstanceObj, true);
                TapLogger.debug(TAG, "injected typeInstanceObj {} takes ", typeInstanceObj, (System.currentTimeMillis() - time));
            }
            initialized.set(true);
        }
    }
}
