package com.tapdata.tm.base.validation.constraints;

import com.tapdata.tm.base.validation.validator.ValueOfEnumValidator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * The annotated element must be in enum. Accepts {@code Enum}.
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/13 6:27 下午
 * @description
 */
@Target({METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER, TYPE_USE})
@Retention(RUNTIME)
@Documented
@Constraint(validatedBy = ValueOfEnumValidator.class)
public @interface ValueOfEnum {

	Class<? extends Enum<?>> enumClass();
	String enumName() default "";
	String message() default "{javax.validation.constraints.ValueOfEnum.message}";
	Class<?>[] groups() default {};
	Class<? extends Payload>[] payload() default {};
}
