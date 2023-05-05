package com.tapdata.tm.commons.util;

import cn.hutool.core.exceptions.ExceptionUtil;

public class ThrowableUtils {
    /**
     * 获取以指定包名为前缀的堆栈信息
     *
     * @param e             异常
     * @param name 包
     * @return 堆栈信息
     */
    public static String getStackTraceByPn(Throwable e, String name) {
        StringBuilder s = new StringBuilder("\n").append(e);
        for (StackTraceElement traceElement : e.getStackTrace()) {
            if (!traceElement.getClassName().contains(name)) {
                continue;
            }
            s.append("\n\tat ").append(traceElement);
        }
        return s.toString();
    }

    public static String getStackTraceByPn(Throwable e) {
        return ExceptionUtil.stacktraceToString(e);
    }
}
