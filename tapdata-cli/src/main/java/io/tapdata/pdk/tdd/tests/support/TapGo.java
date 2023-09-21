package io.tapdata.pdk.tdd.tests.support;

import io.tapdata.pdk.tdd.core.PDKTestBase;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface TapGo {
    boolean goTest() default true;

    int sort() default 0;

    Class<? extends PDKTestBase>[] subTest() default {};

    boolean block() default false;

    boolean isSub() default false;

    String tag() default "default";

    boolean debug() default false;

    boolean ignore() default false;
}
