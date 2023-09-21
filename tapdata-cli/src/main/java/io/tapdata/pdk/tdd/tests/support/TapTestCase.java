package io.tapdata.pdk.tdd.tests.support;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({
        ElementType.METHOD,
        ElementType.TYPE,
        ElementType.ANNOTATION_TYPE,
        ElementType.FIELD,
        ElementType.TYPE_USE,
        ElementType.CONSTRUCTOR,
        ElementType.PARAMETER
})
@Retention(RetentionPolicy.RUNTIME)
public @interface TapTestCase {
    int sort() default 0;

    boolean dump() default false;
}
