package io.tapdata.annotation;

import com.tapdata.entity.DatabaseTypeEnum;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Inherited
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(value = DatabaseTypeAnnotations.class)
public @interface DatabaseTypeAnnotation {
	DatabaseTypeEnum type();
}
