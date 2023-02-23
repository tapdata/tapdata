package io.tapdata.exception;

import java.lang.annotation.*;

/**
 * Use on exception codes class
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/13 15:12 Create
 */
@Inherited
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface TapExClass {
    int code();
    String module();
    String describe() default "";
}
