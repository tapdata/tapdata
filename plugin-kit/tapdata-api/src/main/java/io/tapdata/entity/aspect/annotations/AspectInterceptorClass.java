package io.tapdata.entity.aspect.annotations;

import io.tapdata.entity.aspect.Aspect;

import java.lang.annotation.*;

@Target(value = {ElementType.TYPE})
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
public @interface AspectInterceptorClass {
    Class<? extends Aspect> value();
    int order() default Integer.MAX_VALUE;

    boolean ignoreErrors() default true;
}
