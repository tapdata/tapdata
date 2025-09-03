package com.tapdata.tm.commons.function;

/**
 * 可抛出异常的消费者接口，扩展了标准的 Consumer 接口功能
 * <p>
 * 该接口允许在消费对象时抛出指定类型的异常，提供了比标准 Consumer 更灵活的异常处理能力。
 * 主要用于需要在函数式编程中处理受检异常的场景。
 * </p>
 *
 * @param <T> 消费的对象类型
 * @param <E> 可能抛出的异常类型，必须是 Throwable 的子类
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/11 14:19 Create
 */
public interface ThrowableConsumer<T, E extends Throwable> {

    /**
     * 消费指定的对象，可能抛出异常
     * <p>
     * 该方法接受一个对象参数并对其进行处理，处理过程中可能抛出指定类型的异常。
     * 与标准的 Consumer 不同，此方法显式声明了可能抛出的异常类型。
     * </p>
     *
     * @param o 要消费的对象，类型为 T
     * @throws E 处理过程中可能抛出的异常
     */
    void accept(T o) throws E;
}
