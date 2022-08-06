package io.tapdata.pdk.core.reflection;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;
import io.tapdata.entity.reflection.ClassAnnotationManager;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

import java.net.URL;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Implementation(ClassAnnotationManager.class)
public class ClassAnnotationManagerImpl implements ClassAnnotationManager {
    private static final String TAG = ClassAnnotationManagerImpl.class.getSimpleName();
    private String[] scanPackages;
    private ClassLoader classLoader;
//    private List<URL> urls;

    private final List<ClassAnnotationHandler> classAnnotationHandlers = new CopyOnWriteArrayList<>();

    private void scan() {
        ConfigurationBuilder builder = new ConfigurationBuilder()
                .addScanners(new TypeAnnotationsScanner())
                .addClassLoader(this.classLoader);
//        if(urls != null) {
//            builder.setUrls(urls);
//        }
        if(scanPackages != null) {
            builder.forPackages(this.scanPackages);
        }
        Reflections reflections = new Reflections(builder);

        runClassAnnotationHandlers(reflections);
    }

    void runClassAnnotationHandlers(Reflections reflections) {
        for(ClassAnnotationHandler classAnnotationHandler : classAnnotationHandlers) {
            if(classAnnotationHandler != null && classAnnotationHandler.watchAnnotation() != null) {
                try {
                    classAnnotationHandler.handle(reflections.getTypesAnnotatedWith(classAnnotationHandler.watchAnnotation()));
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                    TapLogger.error(TAG, "Handle class annotation {} failed, {}", classAnnotationHandler.getClass().getSimpleName(), throwable.getMessage());
                }
            }
        }
    }

    @Override
    public ClassAnnotationManager registerClassAnnotationHandler(ClassAnnotationHandler classAnnotationHandler) {
        if(!classAnnotationHandlers.contains(classAnnotationHandler)) {
            classAnnotationHandlers.add(classAnnotationHandler);
        } else {
            TapLogger.warn(TAG, "ClassAnnotationHandler {} already registered, no need register again. ", classAnnotationHandler);
        }
        return this;
    }

    @Override
    public boolean unregisterClassAnnotationHandler(ClassAnnotationHandler classAnnotationHandler) {
        return classAnnotationHandlers.remove(classAnnotationHandler);
    }

    @Override
    public void scan(String[] scanPackages, ClassLoader classLoader) {
        this.scanPackages = scanPackages;
        this.classLoader = classLoader;
        scan();
    }
}
