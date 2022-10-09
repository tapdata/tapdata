package io.tapdata.pdk.core.aspect;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.aspect.*;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationManager;
import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.Callable;
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
            String scanPackage = CommonUtils.getProperty("pdk_aspect_scan_package", "io.tapdata,com.tapdata");
            String[] packages = scanPackage.split(",");
            classAnnotationManager.scan(packages, this.getClass().getClassLoader());

            aspectInterceptorMap = aspectInterceptorAnnotationHandler.getAspectInterceptorsMap();
            aspectObserversMap = aspectObserverAnnotationHandler.getAspectObserversMap();
        }
    }

    public synchronized  <T extends Aspect> void registerAspectInterceptor(Class<T> aspectClass, int order, AspectInterceptor<T> aspectInterceptor) {
        registerAspectInterceptor(aspectClass, order, aspectInterceptor, true);
    }
    @SuppressWarnings("unchecked")
    @Override
    public synchronized  <T extends Aspect> void registerAspectInterceptor(Class<T> aspectClass, int order, AspectInterceptor<T> aspectInterceptor, boolean ignoreErrors) {
        registerInterceptor(aspectClass, order, (AspectInterceptor<Aspect>) aspectInterceptor, ignoreErrors);
    }

    @Override
    public synchronized void registerInterceptor(Class<? extends Aspect> aspectClass, int order, AspectInterceptor<Aspect> aspectInterceptor) {
        registerInterceptor(aspectClass, order, aspectInterceptor, true);
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void registerInterceptor(Class<? extends Aspect> aspectClass, int order, AspectInterceptor<Aspect> aspectInterceptor, boolean ignoreErrors) {
        Collection<AspectInterceptorClassHolder> interceptorClassHolders = aspectInterceptorMap.get(aspectClass);
        if(interceptorClassHolders == null) {
            interceptorClassHolders = Collections.synchronizedSortedSet(new TreeSet<>());
            interceptorClassHolders.add(new AspectInterceptorClassHolder().aspectInterceptor(aspectInterceptor).order(order).ignoreErrors(ignoreErrors));
            aspectInterceptorMap.put(aspectClass, interceptorClassHolders);
            TapLogger.debug(TAG, "(New array) AspectInterceptor {} for Aspect {} will be applied", aspectInterceptor, aspectClass);
        } else {
            Collection<AspectInterceptorClassHolder> newInterceptorClassHolders = Collections.synchronizedSortedSet(new TreeSet<>(interceptorClassHolders));
            for(AspectInterceptorClassHolder aspectInterceptorClassHolder : newInterceptorClassHolders) {
                if(aspectInterceptorClassHolder.getAspectInterceptor() != null && aspectInterceptorClassHolder.getAspectInterceptor().equals(aspectInterceptor)) {
//                    TapLogger.warn(TAG, "aspectInterceptor {} already exists for Aspect {}", aspectInterceptor, aspectClass);
                    return;
                }
            }
            newInterceptorClassHolders.add(new AspectInterceptorClassHolder().aspectInterceptor(aspectInterceptor).order(order));
            aspectInterceptorMap.put(aspectClass, newInterceptorClassHolders);
            aspectInterceptorInstanceMap.putIfAbsent((Class<? extends AspectInterceptor<? extends Aspect>>) aspectInterceptor.getClass(), aspectInterceptor);
            TapLogger.debug(TAG, "(Exist array) AspectInterceptor {} for Aspect {} will be applied", aspectInterceptor, aspectClass);
        }
    }

    @Override
    public synchronized <T extends Aspect> void unregisterAspectInterceptor(Class<T> aspectClass, Class<? extends AspectInterceptor<T>> interceptorClass) {
        Collection<AspectInterceptorClassHolder> interceptorClassHolders = aspectInterceptorMap.get(aspectClass);
        if(interceptorClassHolders != null) {
            Collection<AspectInterceptorClassHolder> newInterceptorClassHolders = Collections.synchronizedSortedSet(new TreeSet<>());
            for(AspectInterceptorClassHolder classHolder : interceptorClassHolders) {
                if(classHolder.getAspectClass().equals(interceptorClass)) {
//                    deleteInterceptorHolders.add(classHolder);
                } else {
                    newInterceptorClassHolders.add(classHolder);
                }
            }
            if(newInterceptorClassHolders.isEmpty())
                aspectInterceptorMap.remove(aspectClass);
            else
                aspectInterceptorMap.put(aspectClass, newInterceptorClassHolders);
            aspectInterceptorInstanceMap.remove(interceptorClass);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized <T extends Aspect> void unregisterAspectInterceptor(Class<T> aspectClass, AspectInterceptor<T> aspectInterceptor) {
        unregisterInterceptor(aspectClass, (AspectInterceptor<Aspect>) aspectInterceptor);
    }
    @Override
    public synchronized void unregisterInterceptor(Class<? extends Aspect> aspectClass, AspectInterceptor<Aspect> aspectInterceptor) {
        Collection<AspectInterceptorClassHolder> interceptorClassHolders = aspectInterceptorMap.get(aspectClass);
        if(interceptorClassHolders != null) {
            Collection<AspectInterceptorClassHolder> newInterceptorClassHolders = Collections.synchronizedSortedSet(new TreeSet<>());
            for(AspectInterceptorClassHolder classHolder : interceptorClassHolders) {
                if(classHolder.getAspectInterceptor() != null && classHolder.getAspectInterceptor().equals(aspectInterceptor)) {
//                    deleteInterceptorHolders.add(classHolder);
                } else {
                    newInterceptorClassHolders.add(classHolder);
                }
            }
            if(newInterceptorClassHolders.isEmpty())
                aspectInterceptorMap.remove(aspectClass);
            else
                aspectInterceptorMap.put(aspectClass, newInterceptorClassHolders);
            aspectInterceptorInstanceMap.remove(aspectInterceptor.getClass());
        }
    }

    @Override
    public synchronized void registerObserver(Class<? extends Aspect> aspectClass, int order, AspectObserver<Aspect> aspectObserver) {
        registerObserver(aspectClass, order, aspectObserver, true);
    }
    @Override
    public synchronized void registerObserver(Class<? extends Aspect> aspectClass, int order, AspectObserver<Aspect> aspectObserver, boolean ignoreErrors) {
        Collection<AspectObserverClassHolder> observerClassHolders = aspectObserversMap.get(aspectClass);
        if(observerClassHolders == null) {
            observerClassHolders = Collections.synchronizedSortedSet(new TreeSet<>());
            observerClassHolders.add(new AspectObserverClassHolder().aspectObserver(aspectObserver).order(order).ignoreErrors(ignoreErrors));
            aspectObserversMap.put(aspectClass, observerClassHolders);
            TapLogger.debug(TAG, "(New array) AspectObserver {} for Aspect {} will be applied", aspectObserver, aspectClass);
        } else {
            Collection<AspectObserverClassHolder> newObserverClassHolders = Collections.synchronizedSortedSet(new TreeSet<>(observerClassHolders));
            for(AspectObserverClassHolder aspectObserverClassHolder : newObserverClassHolders) {
                if(aspectObserverClassHolder.getAspectObserver() != null && aspectObserverClassHolder.getAspectObserver().equals(aspectObserver)) {
//                    TapLogger.warn(TAG, "aspectObserver {} already exists for Aspect {}", aspectObserver, aspectClass);
                    return;
                }
            }
            newObserverClassHolders.add(new AspectObserverClassHolder().aspectObserver(aspectObserver).order(order).ignoreErrors(ignoreErrors));
            aspectObserversMap.put(aspectClass, newObserverClassHolders);
            aspectObserverInstanceMap.putIfAbsent((Class<? extends AspectObserver<? extends Aspect>>) aspectObserver.getClass(), aspectObserver);
            TapLogger.debug(TAG, "(Exist array) AspectObserver {} for Aspect {} will be applied", aspectObserver, aspectClass);
        }
    }

    public synchronized <T extends Aspect> void registerAspectObserver(Class<T> aspectClass, int order, AspectObserver<T> aspectObserver) {
        registerAspectObserver(aspectClass, order, aspectObserver, true);
    }
    @SuppressWarnings("unchecked")
    @Override
    public synchronized <T extends Aspect> void registerAspectObserver(Class<T> aspectClass, int order, AspectObserver<T> aspectObserver, boolean ignoreErrors) {
        registerObserver(aspectClass, order, (AspectObserver<Aspect>) aspectObserver, ignoreErrors);
    }

    @Override
    public synchronized  <T extends Aspect> void unregisterAspectObserver(Class<T> aspectClass, Class<? extends AspectObserver<T>> observerClass) {
        Collection<AspectObserverClassHolder> observerClassHolders = aspectObserversMap.get(aspectClass);
        if(observerClassHolders != null) {
            Collection<AspectObserverClassHolder> newObserverClassHolders = Collections.synchronizedSortedSet(new TreeSet<>());
            boolean found = false;
            for(AspectObserverClassHolder classHolder : observerClassHolders) {
                if(classHolder.getAspectClass().equals(observerClass)) {
//                    deleteObserverHolders.add(classHolder);
                    found = true;
                } else {
                    newObserverClassHolders.add(classHolder);
                }
            }
            if(found) {
                aspectObserversMap.put(aspectClass, newObserverClassHolders);
                aspectObserverInstanceMap.remove(observerClass);
            }
        }
    }

    @Override
    public synchronized  <T extends Aspect> void unregisterAspectObserver(Class<T> aspectClass, AspectObserver<T> aspectObserver) {
        unregisterObserver(aspectClass, (AspectObserver<Aspect>) aspectObserver);
    }
    @Override
    public synchronized  void unregisterObserver(Class<? extends Aspect> aspectClass, AspectObserver<Aspect> aspectObserver) {
        Collection<AspectObserverClassHolder> observerClassHolders = aspectObserversMap.get(aspectClass);
        if(observerClassHolders != null) {
            Collection<AspectObserverClassHolder> newObserverClassHolders = Collections.synchronizedSortedSet(new TreeSet<>());
            boolean found = false;
            for(AspectObserverClassHolder classHolder : observerClassHolders) {
                if(classHolder.getAspectObserver() != null && classHolder.getAspectObserver().equals(aspectObserver)) {
//                    deleteObserverHolders.add(classHolder);
                    found = true;
                } else {
                    newObserverClassHolders.add(classHolder);
                }
            }
            if(found) {
                if(newObserverClassHolders.isEmpty())
                    aspectObserversMap.remove(aspectClass);
                else
                    aspectObserversMap.put(aspectClass, newObserverClassHolders);
                aspectObserverInstanceMap.remove(aspectObserver.getClass());
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends AspectObserver<?>> T getAspectObserver(Class<T> observerClass) {
        return (T) aspectObserverInstanceMap.get(observerClass);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends AspectInterceptor<?>> T getAspectInterceptor(Class<T> interceptorClass) {
        return (T) aspectInterceptorInstanceMap.get(interceptorClass);
    }

    @Override
    public <T extends Aspect> void executeAspectObservers(Class<T> aspectClass, Callable<T> aspectCallable) {
        if(aspectClass != null && aspectCallable != null) {
            Collection<AspectObserverClassHolder> observerClasses = aspectObserversMap.get(aspectClass);
            if(observerClasses != null && !observerClasses.isEmpty()) {
                try {
                    T aspect = aspectCallable.call();
                    executeObservers(aspect, observerClasses);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        }
    }

    @Override
    public void executeAspectObservers(Aspect aspect) {
        if(aspectObserversMap != null && aspect != null) {
            Collection<AspectObserverClassHolder> observerClasses = aspectObserversMap.get(aspect.getClass());
            if(observerClasses != null) {
                executeObservers(aspect, observerClasses);
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void executeObservers(Aspect aspect, Collection<AspectObserverClassHolder> observerClasses) {
        for(AspectObserverClassHolder observerClass : observerClasses) {
            AspectObserver observer = observerClass.getAspectObserver();
            if(observer == null) {
                observer = aspectObserverInstanceMap.computeIfAbsent(observerClass.getAspectClass(), aClass -> {
                    try {
                        AspectObserver<? extends Aspect> aspectObserver = observerClass.getAspectClass().getConstructor().newInstance();
                        InstanceFactory.injectBean(aspectObserver);
                        return aspectObserver;
                    } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                             NoSuchMethodException e) {
                        throw new RuntimeException("Instantiate AspectObserver sub class " + observerClass + " failed, " + e.getMessage());
                    }
                });
            }

            AspectObserver finalObserver = observer;
            if(observerClass.isIgnoreErrors()) {
                CommonUtils.ignoreAnyError(() -> {
                    finalObserver.observe(aspect);
                }, TAG);
            } else {
                finalObserver.observe(aspect);
            }
        }
    }

    @Override
    public <T extends Aspect> AspectInterceptResult executeAspectInterceptors(Class<T> aspectClass, Callable<T> aspectCallable) {
        if(aspectClass != null && aspectCallable != null) {
            Collection<AspectInterceptorClassHolder> observerClasses = aspectInterceptorMap.get(aspectClass);
            if(observerClasses != null && !observerClasses.isEmpty()) {
                try {
                    T aspect = aspectCallable.call();
                    return getInterceptResult(aspect, observerClasses);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }


    @Override
    public AspectInterceptResult executeAspectInterceptors(Aspect aspect) {
        if(aspectInterceptorMap != null && aspect != null) {
            Collection<AspectInterceptorClassHolder> observerClasses = aspectInterceptorMap.get(aspect.getClass());
            if(observerClasses != null) {
                return getInterceptResult(aspect, observerClasses);
            }
        }
        return null;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private AspectInterceptResult getInterceptResult(Aspect aspect, Collection<AspectInterceptorClassHolder> observerClasses) {
        AtomicReference<AspectInterceptResult> resultAtomicReference = new AtomicReference<>();
        for(AspectInterceptorClassHolder interceptClass : observerClasses) {
            AspectInterceptor interceptor = interceptClass.getAspectInterceptor();
            if(interceptor == null) {
                interceptor = aspectInterceptorInstanceMap.computeIfAbsent(interceptClass.getAspectClass(), aClass -> {
                    try {
                        return interceptClass.getAspectClass().getConstructor().newInstance();
                    } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                             NoSuchMethodException e) {
                        throw new RuntimeException("Instantiate AspectInterceptor sub class " + interceptClass + " failed, " + e.getMessage());
                    }
                });
            }
            AspectInterceptor finalInterceptor = interceptor;
            if(interceptClass.isIgnoreErrors()) {
                CommonUtils.ignoreAnyError(() -> {
                    resultAtomicReference.set(finalInterceptor.intercept(aspect));
                }, TAG);
            } else {
                resultAtomicReference.set(finalInterceptor.intercept(aspect));
            }
            if(resultAtomicReference.get() != null && resultAtomicReference.get().isIntercepted()) {
                TapLogger.debug(TAG, "Aspect {} intercepted {}", aspect, resultAtomicReference.get());
                break;
            }
        }
        return resultAtomicReference.get();
    }

    @Override
    public <T extends Aspect> boolean hasInterceptorOrObserver(Class<T> aspectClass) {
        Collection<AspectInterceptorClassHolder> interceptorClassHolders = aspectInterceptorMap.get(aspectClass);
        Collection<AspectObserverClassHolder> observerClassHolders = aspectObserversMap.get(aspectClass);
        return (interceptorClassHolders != null && !interceptorClassHolders.isEmpty()) || (observerClassHolders != null && !observerClassHolders.isEmpty());
    }

    /**
     *
     *
     * @param aspectClass
     * @param aspectCallable
     * @return
     * @param <T>
     */
    @Override
    public <T extends Aspect> AspectInterceptResult executeAspect(Class<T> aspectClass, Callable<T> aspectCallable) {
        if(aspectClass != null && aspectCallable != null) {
            Collection<AspectInterceptorClassHolder> interceptorClassHolders = aspectInterceptorMap.get(aspectClass);
            if(interceptorClassHolders != null && !interceptorClassHolders.isEmpty()) {
                T aspect = null;
                try {
                    aspect = aspectCallable.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if(aspect != null) {
                    AspectInterceptResult interceptResult = getInterceptResult(aspect, interceptorClassHolders);
                    if(interceptResult == null || !interceptResult.isIntercepted()) {
                        executeAspectObservers(aspect);
                    } else {
                        return interceptResult;
                    }
                }
            } else {
                Collection<AspectObserverClassHolder> observerClassHolders = aspectObserversMap.get(aspectClass);
                if(observerClassHolders != null && !observerClassHolders.isEmpty()) {
                    T aspect = null;
                    try {
                        aspect = aspectCallable.call();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    executeObservers(aspect, observerClassHolders);
                }
            }
        }
        return null;
    }

    public <T extends Aspect> AspectInterceptResult executeAspectInterceptors1(Class<T> aspectClass, Callable<T> aspectCallable) {
        if(aspectClass != null && aspectCallable != null) {
            Collection<AspectInterceptorClassHolder> observerClasses = aspectInterceptorMap.get(aspectClass);
            if(observerClasses != null && !observerClasses.isEmpty()) {
                try {
                    T aspect = aspectCallable.call();
                    return getInterceptResult(aspect, observerClasses);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }

    @Override
    public <T extends Aspect> AspectInterceptResult executeAspect(T aspect) {
        AspectInterceptResult aspectInterceptResult = executeAspectInterceptors(aspect);
        if(aspectInterceptResult == null || !aspectInterceptResult.isIntercepted())
            executeAspectObservers(aspect);
        return aspectInterceptResult;
    }

    private <T extends Aspect> Callable<T> getAspectCallable(Callable<T> aspectCallable, AtomicReference<T> aspectRef) {
        return () -> {
            if (aspectRef.get() == null) {
                aspectRef.set(aspectCallable.call());
            }
            return aspectRef.get();
        };
    }
}
