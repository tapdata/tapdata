package com.tapdata.tm.config;

import org.springframework.data.annotation.Persistent;
import org.springframework.data.mongodb.core.annotation.Collation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Persistent
@Collation
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface CappedCollection {
    boolean capped() default true;

    long maxLength() default 100;

    long maxMemory() default 1024*1024*1024;
}
