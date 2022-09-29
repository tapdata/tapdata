package io.tapdata.entity.reflection;

import io.tapdata.entity.error.CoreException;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
public abstract class FieldAnnotationHandler<T extends Annotation> {
    public abstract Class<T> annotationClass();

    public abstract Object inject(Annotation annotation, Field field) throws CoreException;

}
