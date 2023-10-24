package io.tapdata.entity.reflection;

public interface ClassAnnotationManager {
   ClassAnnotationManager registerClassAnnotationHandler(ClassAnnotationHandler classAnnotationHandler);
   boolean unregisterClassAnnotationHandler(ClassAnnotationHandler classAnnotationHandler);

    void scan(String[] scanPackages, ClassLoader classLoader);
}
