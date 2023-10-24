package io.tapdata.pdk.core.classloader;

import io.tapdata.entity.reflection.ClassAnnotationHandler;

import java.io.File;

public interface JarAnnotationHandlersListener {
    /**
     * Each jar file may have corresponding ClassAnnotationHandlers.
     * If jar file changes, this method may be called.
     *
     * @param jarFile
     * @return
     */
    ClassAnnotationHandler[] annotationHandlers(File jarFile);
}
