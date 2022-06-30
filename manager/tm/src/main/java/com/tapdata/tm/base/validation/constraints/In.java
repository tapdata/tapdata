package com.tapdata.tm.base.validation.constraints;

import com.tapdata.tm.base.validation.validator.InValidator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * The annotated element must be in spec array. Accepts {@code Object[]}.
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/10/20 11:56 上午
 * @description
 */
@Target({METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER, TYPE_USE})
@Retention(RUNTIME)
@Documented
@Constraint(validatedBy = InValidator.class)
public @interface In {

	String[] strings() default {};
	int[] numbers() default {};
	String message() default "{javax.validation.constraints.In.message}";
	Class<?>[] groups() default {};
	Class<? extends Payload>[] payload() default {};
}
