package io.tapdata.annotation;

import java.lang.annotation.*;

@Inherited
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface StageAnnotations {
    StageAnnotation[] value() default {};
}
