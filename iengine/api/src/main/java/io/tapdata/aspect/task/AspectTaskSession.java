package io.tapdata.aspect.task;

import java.lang.annotation.*;

@Target(value = {ElementType.TYPE})
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
public @interface AspectTaskSession {
	String value() default "default";
	int order() default Integer.MAX_VALUE;
}
