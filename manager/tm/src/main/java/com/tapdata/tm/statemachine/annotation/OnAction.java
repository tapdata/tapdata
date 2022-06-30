package com.tapdata.tm.statemachine.annotation;

import com.tapdata.tm.statemachine.enums.Transitions;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.core.annotation.AliasFor;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface OnAction {

	@AliasFor("transitions")
	Transitions[] value() default {};

	@AliasFor("value")
	Transitions[] transitions() default {};
}
