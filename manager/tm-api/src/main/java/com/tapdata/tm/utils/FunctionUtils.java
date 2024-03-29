package com.tapdata.tm.utils;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.util.ThrowableUtils;
import com.tapdata.tm.task.service.BranchHandle;
import com.tapdata.tm.task.service.ThrowExceptionFunction;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.error.QuiteException;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Slf4j
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

    public static void ignoreAnyError(AnyError runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            log.warn(ThrowableUtils.getStackTraceByPn(e));
        }
    }

    public interface AnyError {
        void run() throws Throwable;
    }
}
