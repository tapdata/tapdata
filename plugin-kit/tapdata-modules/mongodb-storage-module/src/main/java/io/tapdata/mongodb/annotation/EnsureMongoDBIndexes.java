package io.tapdata.mongodb.annotation;

import java.lang.annotation.*;

@Target(value = ElementType.TYPE)
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
public @interface EnsureMongoDBIndexes {
    EnsureMongoDBIndex[] value();
}
