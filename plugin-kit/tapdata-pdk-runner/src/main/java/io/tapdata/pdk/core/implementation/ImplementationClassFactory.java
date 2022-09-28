package io.tapdata.pdk.core.implementation;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.entity.reflection.ClassAnnotationHandler;
import io.tapdata.pdk.core.utils.AnnotationUtils;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

import java.net.URL;
import java.util.Collection;
import java.util.List;

public class ImplementationClassFactory {
    private static final String TAG = ImplementationClassFactory.class.getSimpleName();
    private String[] scanPackages;
    private ClassLoader classLoader;
    private List<URL> urls;
    private ImplementationAnnotationHandler implementationAnnotationHandler;
    private BeanAnnotationHandler beanAnnotationHandler;

    public ImplementationClassFactory() {
    }

    public void init() {
        init(null, this.getClass().getClassLoader(), null);
    }

    public void init(String[] scanPackages) {
        init(scanPackages, this.getClass().getClassLoader(), null);
    }

    public void init(String[] scanPackages, ClassLoader classLoader) {
        init(scanPackages, classLoader, null);
    }

    public void init(String[] scanPackages, ClassLoader classLoader, List<URL> urls) {
        this.scanPackages = scanPackages;
        this.classLoader = classLoader;
        this.urls = urls;

        implementationAnnotationHandler = new ImplementationAnnotationHandler();
        beanAnnotationHandler = new BeanAnnotationHandler();
        scan();
//        implementationAnnotationHandler.apply();
    }

    private void scan() {
        ConfigurationBuilder builder = new ConfigurationBuilder()
                .addScanners(new TypeAnnotationsScanner())
//                .forPackages(this.scanPackages)
                .addClassLoader(this.classLoader);
        if(urls != null) {
            builder.setUrls(urls);
        }
        if(scanPackages != null) {
//            builder.filterInputsBy(new FilterBuilder()
//                    .includePackage(this.scanPackages)
//            );
            builder.forPackages(this.scanPackages);
        }
        Reflections reflections = new Reflections(builder);
        TapLogger.debug(TAG, "Start scanning implementation classes");
        AnnotationUtils.runClassAnnotationHandlers(reflections, new ClassAnnotationHandler[]{
                implementationAnnotationHandler,
                beanAnnotationHandler
        }, TAG);
    }

    private ImplClasses getImplementationClassHolder(Class<?> interfaceClass) {
        if(implementationAnnotationHandler == null)
            return null;
        return implementationAnnotationHandler.getImplementationClassHolder(interfaceClass);
    }

    public Class<?> getImplementationClass(Class<?> interfaceClass) {
        ImplClasses classHolder = implementationAnnotationHandler.getImplementationClassHolder(interfaceClass);
        if(classHolder != null) {
            return classHolder.anyImplementationClass();
        }
        return null;
    }

    public Class<?> getImplementationClass(Class<?> interfaceClass, String type) {
        ImplClasses implClasses = getImplementationClassHolder(interfaceClass);
        if(implClasses != null) {
            ImplClass implClass =  implClasses.getTypeClassHolderMap().get(type);
            if(implClass != null) {
                return implClass.getClazz();
            }
        }
        return null;
    }

    public boolean hasType(Class<?> interfaceClass, String type) {
        ImplClasses implClasses = getImplementationClassHolder(interfaceClass);
        if(implClasses == null) {
            return false;
        }
        ImplClass implClass = implClasses.getImplementationClass(type);
        return implClass != null;
    }

    public Collection<String> getTypes(Class<?> interfaceClass) {
        ImplClasses implClasses = getImplementationClassHolder(interfaceClass);
        if(implClasses != null) {
            return implClasses.getTypeClassHolderMap().keySet();
        }
        return null;
    }

    public <T> T create(Class<T> interfaceClass) {
        ImplClasses implClasses = getImplementationClassHolder(interfaceClass);
        if(implClasses != null) {
            Class<?> implClass = implClasses.anyImplementationClass();
            if(implClass != null) {
                try {
                    return (T) implClass.getConstructor().newInstance();
                } catch (Throwable e) {
                    TapLogger.error(TAG, "Create any implementation class {} for interface {} failed, {}", implClass, interfaceClass, e.getMessage());
                }
            }
        }
        return null;
//        throw new CoreException(PDKRunnerErrorCodes.IMPL_CREATE_FAILED, "Create failed, no implementation for interfaceClass " + interfaceClass);
    }

    public <T> T create(Class<T> interfaceClass, String type) {
        ImplClasses implClasses = getImplementationClassHolder(interfaceClass);
        if(implClasses != null) {
            ImplClass implClass = implClasses.getImplementationClass(type);
            if(implClass != null) {
                try {
                    return (T) implClass.getClazz().getConstructor().newInstance();
                } catch (Throwable e) {
                    TapLogger.error(TAG, "Create implementation class {} for interface {} type {} failed, {}", implClass.getClazz(), interfaceClass, type, e.getMessage());
                }
            }
        }
        return null;
//        throw new CoreException(PDKRunnerErrorCodes.IMPL_CREATE_TYPE_FAILED, "Create failed, no implementation for interfaceClass " + interfaceClass + " type " + type);
    }
}
