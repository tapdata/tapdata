package com.tapdata.tm.commons.function;

/**
 * 带有异常抛出功能的函数式接口
 *
 * @param <R> 函数返回值类型
 * @param <T> 函数输入参数类型
 * @param <E> 函数可能抛出的异常类型
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/4/14 17:53 Create
 */
public interface ThrowableFunction<R, T, E extends Throwable> {
    /**
     * 应用函数到给定的参数上
     *
     * @param t 输入参数
     * @return 函数计算结果
     * @throws E 可能抛出的异常
     */
    R apply(T t) throws E;
}
