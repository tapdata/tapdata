package io.tapdata.annotation;

import java.lang.annotation.*;

/**
 * @author samuel
 * @Description
 * @create 2022-02-09 00:13
 **/
@Inherited
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Ignore {
}
