package com.tapdata.tm.utils;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.task.service.BranchHandle;
import com.tapdata.tm.task.service.ThrowExceptionFunction;

public class FunctionUtils {
    /**
     * 参数为true或false时，分别进行不同的操作
     *
     * @param b
     * @return com.example.demo.func.BranchHandle
     **/
    public static BranchHandle isTureOrFalse(boolean b){

        return (trueHandle, falseHandle) -> {
            if (b){
                trueHandle.run();
            } else {
                falseHandle.run();
            }
        };
    }

    /**
     *  如果参数为true抛出异常
     *
     * @param b
     * @return com.example.demo.func.ThrowExceptionFunction
     **/
    public static ThrowExceptionFunction isTure(boolean b){

        return (errorCode, arg) -> {
            if (b){
                throw new BizException(errorCode, arg);
            }
        };
    }
}
