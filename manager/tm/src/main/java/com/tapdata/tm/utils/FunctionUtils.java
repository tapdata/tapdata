package com.tapdata.tm.utils;

import com.tapdata.tm.task.service.BranchHandle;

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
}
