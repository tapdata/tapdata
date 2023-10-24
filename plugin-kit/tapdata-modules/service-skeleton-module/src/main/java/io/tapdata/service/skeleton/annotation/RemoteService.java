package io.tapdata.service.skeleton.annotation;

import java.lang.annotation.*;

@Target(value = {ElementType.TYPE})
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
public @interface RemoteService {
    int concurrentLimit() default -1;
    int waitingSize() default 20000;
}
