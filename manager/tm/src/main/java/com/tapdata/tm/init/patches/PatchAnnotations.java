package com.tapdata.tm.init.patches;

import java.lang.annotation.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/2/14 20:36 Create
 */
@Inherited
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface PatchAnnotations {
    PatchAnnotation[] value() default {};
}
