package com.tapdata.tm.commons.util;

import cn.hutool.core.exceptions.ExceptionUtil;

import java.util.function.Function;

/**
 * 异常处理工具类
 * 提供异常堆栈过滤、异常收集等功能
 */
public class ThrowableUtils {

    /**
     * 获取以指定包名为前缀的堆栈信息
     *
     * @param e    异常对象
     * @param name 包名前缀，用于过滤堆栈信息
     * @return 过滤后的堆栈信息字符串，只包含指定包名的堆栈元素
     */
    public static String getStackTraceByPn(Throwable e, String name) {
        StringBuilder s = new StringBuilder("\n").append(e);
        // 遍历堆栈跟踪元素
        for (StackTraceElement traceElement : e.getStackTrace()) {
            // 如果类名不包含指定包名，则跳过
            if (!traceElement.getClassName().contains(name)) {
                continue;
            }
            // 添加符合条件的堆栈元素到结果中
            s.append("\n\tat ").append(traceElement);
        }
        return s.toString();
    }

    /**
     * 获取异常的单行堆栈跟踪字符串
     *
     * @param e 异常对象
     * @return 异常的单行堆栈跟踪字符串
     */
    public static String getStackTraceByPn(Throwable e) {
        return ExceptionUtil.stacktraceToOneLineString(e);
    }

    /**
     * 创建默认的异常收集器实例
     *
     * @param <T> 继承自Collector的类型
     * @return 默认的异常收集器实例
     */
    public static <T extends ThrowableCollector<Throwable>> T collector() {
        ThrowableCollector<Throwable> collector = new ThrowableCollector<>() {
            @Override
            protected Throwable parseThrowable(Throwable e) {
                return e; // 直接返回原始异常
            }
        };
        return (T) collector;
    }

    /**
     * 创建自定义异常转换函数的异常收集器实例
     *
     * @param fn  异常转换函数，用于将Throwable转换为指定类型的异常
     * @param <T> 继承自Collector的类型
     * @param <E> 异常类型
     * @return 配置了转换函数的异常收集器实例
     */
    public static <T extends ThrowableCollector<E>, E extends Throwable> T collector(Function<Throwable, E> fn) {
        ThrowableCollector<E> collector = new ThrowableCollector<>() {
            @Override
            protected E parseThrowable(Throwable e) {
                return fn.apply(e); // 使用提供的函数转换异常
            }
        };
        return (T) collector;
    }

}
