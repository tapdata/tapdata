package com.tapdata.tm.commons.function;

/**
 * 可抛出异常的 Runnable 接口，扩展了标准的 Runnable 接口功能
 * <p>
 * 该接口允许在执行任务时抛出指定类型的异常，提供了比标准 Runnable 更灵活的异常处理能力。
 * 主要用于需要在函数式编程中处理受检异常的场景，特别是在线程执行或定时任务中。
 * </p>
 *
 * @param <E> 可能抛出的异常类型，必须是 Throwable 的子类
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/11 14:19 Create
 */
public interface ThrowableRunnable<E extends Throwable> {

    /**
     * 执行任务，可能抛出异常
     * <p>
     * 该方法不接受任何参数，执行特定的任务逻辑，处理过程中可能抛出指定类型的异常。
     * 与标准的 Runnable 不同，此方法显式声明了可能抛出的异常类型。
     * </p>
     *
     * @throws E 执行过程中可能抛出的异常
     */
    void run() throws E;
}
