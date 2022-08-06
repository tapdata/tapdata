package io.tapdata.pdk.core.utils;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;
import org.reflections.Reflections;

public class AnnotationUtils {
    public static void runClassAnnotationHandlers(Reflections reflections, ClassAnnotationHandler[] handlers, String tag) {
        if(handlers != null) {
            for(ClassAnnotationHandler classAnnotationHandler : handlers) {
                if(classAnnotationHandler != null && classAnnotationHandler.watchAnnotation() != null) {
                    try {
                        classAnnotationHandler.handle(reflections.getTypesAnnotatedWith(classAnnotationHandler.watchAnnotation()));
                    } catch (Throwable throwable) {
                        throwable.printStackTrace();
                        TapLogger.error(tag, "Handle class annotation {} failed, {}", classAnnotationHandler.getClass().getSimpleName(), throwable.getMessage());
                    }
                }
            }
        }
    }

}
