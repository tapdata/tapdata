package com.tapdata.tm.task.service;

/**
 * 抛异常接口
 **/
@FunctionalInterface
public interface ThrowExceptionFunction {

    /**
     * 抛出异常信息
     *
     * @param errorCode code
     * @param arg 异常信息
     **/
    void throwMessage(String errorCode, Object... arg);
}
