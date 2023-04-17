package com.tapdata.tm.commons.function;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/4/14 17:53 Create
 */
public interface ThrowableFunction<R, T, E extends Throwable> {
    R apply(T t) throws E;
}
