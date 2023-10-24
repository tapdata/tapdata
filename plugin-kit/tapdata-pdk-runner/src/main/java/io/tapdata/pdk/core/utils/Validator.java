package io.tapdata.pdk.core.utils;

import io.tapdata.entity.error.CoreException;

import java.util.Arrays;
import java.util.List;

public class Validator {
    private Validator() {}

    public static void checkListNotNull(int errorCode, List<?> param) {
        if(param == null || param.isEmpty())
            throw new CoreException(errorCode, "Illegal params");
    }

    public static void checkAnyNotNull(int errorCode, Object... params) {
        if(params != null) {
            for(Object it : params) {
                if(it != null)
                    return;
            }
        }
        throw new CoreException(errorCode, "Illegal params, params: " + Arrays.toString(params));
    }

    public static void checkAllNotNull(int errorCode, Object... params) {
        if(params != null) {
            for(Object it : params) {
                if(it == null)
                    throw new CoreException(errorCode, "Illegal params, params: " + Arrays.toString(params));
            }
        }
    }

    public static void checkNotNull(int errorCode, Object param) {
        if(param == null)
            throw new CoreException(errorCode, "Illegal params");
    }

    public static Object checkWithDefault(Object param, Object defaultValue) {
        if(param == null)
            return defaultValue;
        return param;
    }
}
