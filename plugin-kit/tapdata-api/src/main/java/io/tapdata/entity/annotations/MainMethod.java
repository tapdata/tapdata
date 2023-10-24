package io.tapdata.entity.annotations;

import java.lang.annotation.*;

@Target(value = {ElementType.TYPE})
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
public @interface MainMethod {
	String value() default "";
	int order() default Integer.MIN_VALUE;
}