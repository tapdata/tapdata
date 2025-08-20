package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.function.ThrowableRunnable;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * 异常收集器抽象类
 * 用于收集多个异常并将它们作为抑制异常添加到主异常中
 *
 * @param <E> 异常类型
 */
public abstract class ThrowableCollector<E extends Throwable> {
    // 使用原子引用存储收集到的异常
    private final AtomicReference<E> error;

    /**
     * 默认构造函数，创建一个新的原子引用
     */
    public ThrowableCollector() {
        this(new AtomicReference<>());
    }

    /**
     * 构造函数，使用指定的原子引用
     *
     * @param error 用于存储异常的原子引用
     */
    public ThrowableCollector(AtomicReference<E> error) {
        this.error = error;
    }

    /**
     * 解析Throwable的抽象方法，由子类实现具体的异常转换逻辑
     *
     * @param e 原始异常
     * @return 转换后的异常
     */
    protected abstract E parseThrowable(Throwable e);

    /**
     * 获取收集到的异常
     *
     * @return 收集到的异常，如果没有则返回null
     */
    public E get() {
        return error.get();
    }

    /**
     * 收集异常的方法，执行可能抛出异常的操作
     *
     * @param runnable 可能抛出异常的操作
     * @return 当前收集器实例，支持链式调用
     */
    public <T extends ThrowableCollector<E>> T collect(ThrowableRunnable<E> runnable) {
        try {
            runnable.run(); // 执行操作
        } catch (Throwable e) {
            E ex = parseThrowable(e); // 转换异常
            // 如果error为null，则设置为当前异常；否则将当前异常作为抑制异常添加
            if (!error.compareAndSet(null, ex)) {
                get().addSuppressed(ex);
            }
        }
        return (T) this;
    }

    /**
     * 执行可能抛出异常的操作，如果存在异常则抛出
     *
     * @throws E 收集到的异常（如果存在）
     */
    public void throwIfPresent() throws E {
        E throwable = get(); // 收集异常并获取
        if (null != throwable) {
            throw throwable; // 如果存在异常则抛出
        }
    }

    /**
     * 对收集到的异常执行消费操作
     *
     * @param errorConsumer 异常消费者函数
     */
    public void consume(Consumer<E> errorConsumer) {
        E ex = get(); // 获取收集到的异常
        if (null != ex) {
            errorConsumer.accept(ex); // 如果存在异常，则消费它
        }
    }
}
