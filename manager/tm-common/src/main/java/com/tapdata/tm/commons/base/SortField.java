package com.tapdata.tm.commons.base;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SortField {
    boolean support() default true;

    String[] name();

    boolean normal() default false;
}
