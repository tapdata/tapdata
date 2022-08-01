package com.tapdata.tm.task.service;

@FunctionalInterface
public interface BranchHandle {
    /**
     * 分支操作
     *
     * @param trueHandle 为true时要进行的操作
     * @param falseHandle 为false时要进行的操作
     **/
    void trueOrFalseHandle(Runnable trueHandle, Runnable falseHandle);
}
