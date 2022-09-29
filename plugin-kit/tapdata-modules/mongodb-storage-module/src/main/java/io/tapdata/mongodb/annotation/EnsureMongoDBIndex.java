package io.tapdata.mongodb.annotation;

import java.lang.annotation.*;
import java.util.Map;

@Target(value = ElementType.TYPE)
@Retention(value = RetentionPolicy.RUNTIME)
@Repeatable(value = EnsureMongoDBIndexes.class)
@Documented
public @interface EnsureMongoDBIndex {
    String value();
    boolean background() default false;
    boolean unique() default false;
    boolean sparse() default false;
    long expireAfterSeconds() default -1;
}
