package io.tapdata.entity.annotations;

import java.lang.annotation.*;

@Target(value = {ElementType.TYPE})
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
public @interface Implementation {
  Class<?> value();

    /**
     * The build number of the implementation
     * Higher build number with the same type will replace the lower one.
     *
     * @return
     */
  int buildNumber() default 1;

    /**
     * The type of the implementation
     *
     * @return
     */
  String type() default "default";
}
