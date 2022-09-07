package io.tapdata.mongodb.annotation;

import java.lang.annotation.*;

@Target(value = ElementType.TYPE)
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
public @interface MongoDAO {
    /**
     * 库名
     * @return
     */
    String dbName();
    
    /**
     * 表名
     * @return
     */
    String collectionName() default "";
}
