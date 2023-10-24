package io.tapdata.exception;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Use on static filed with exception code
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/13 15:12 Create
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TapExCode {
	String describe() default "";

	String describeCN() default "";

	String solution() default "";

	String solutionCN() default "";

	boolean recoverable() default false;
	boolean skippable() default false;

	TapExLevel level() default TapExLevel.NORMAL;

	TapExType type() default TapExType.RUNTIME;

	Class<? extends Exception> relateException() default RuntimeException.class;

	String howToReproduce() default "";

	String[] seeAlso() default {"https://docs.tapdata.io/"};
}
