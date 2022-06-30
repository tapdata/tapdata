package com.tapdata.tm.base.validation.constraints;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/13 6:13 下午
 * @description
 */
@Target({ METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER, TYPE_USE })
@Retention(RUNTIME)
@Documented
@Constraint(validatedBy = {})
public @interface CustomerTypeSubset {
	String[] anyOf();
	String message() default "{javax.validation.constraints.CustomerTypeSubset.message}";
	Class<?>[] groups() default {};
	Class<? extends Payload>[] payload() default {};
}
