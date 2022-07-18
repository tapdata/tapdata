package io.tapdata.annotation;

import com.tapdata.entity.DatabaseTypeEnum;

import java.lang.annotation.*;

@Inherited
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(value = DatabaseTypeAnnotations.class)
public @interface DatabaseTypeAnnotation {
	DatabaseTypeEnum type();
}
