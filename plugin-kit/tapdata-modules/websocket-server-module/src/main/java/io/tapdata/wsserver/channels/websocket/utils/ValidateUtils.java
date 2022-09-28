package io.tapdata.wsserver.channels.websocket.utils;

import io.tapdata.entity.error.CoreException;
import io.tapdata.wsserver.channels.error.WSErrors;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;

public class ValidateUtils {
    public static void checkOffset(Integer offset) {
        if(offset == null)
            throw new CoreException(WSErrors.ERROR_ILLEGAL_OFFSET, "offset can not be null");

        if(offset < 0)
            throw new CoreException(WSErrors.ERROR_ILLEGAL_OFFSET, "offset can not be negative");
    }

    public static int checkLimit(Integer limit) {
        if(limit == null)
            throw new CoreException(WSErrors.ERROR_ILLEGAL_LIMIT, "limit can not be null");

        if(limit < 0)
            throw new CoreException(WSErrors.ERROR_ILLEGAL_LIMIT, "limit can not be negative");

        if(limit > 1000)
            limit = 1000;
        return limit;
    }

    public static void checkListNotNull(List<?> param) {
        if(param == null || param.isEmpty())
            throw new CoreException(WSErrors.ERROR_ILLEGAL_PARAMETERS, "Illegal parameters");
    }

    public static void checkNotNull(Object param) {
        if(param == null)
            throw new CoreException(WSErrors.ERROR_ILLEGAL_PARAMETERS, "Illegal parameters");
    }

    public static Object checkWithDefault(Object param, Object defaultValue) {
        if(param == null)
            return defaultValue;
        return param;
    }

    public static void checkSortType(Integer integer) {
        if (integer != -1 || integer != 1) {
            throw new CoreException(WSErrors.ERROR_ILLEGAL_SORT, "sort type $integer is invalid");
        }
    }

    public static void checkAnyNotNull(Object... params) {
        if (null != params){
            for(Object it : params) {
                if(it != null)
                    return;
            }
        }
        throw new CoreException(WSErrors.ERROR_ILLEGAL_PARAMETERS, "Illegal parameters");
    }

    public static void checkAllNotNull(Object... params) {
        if (Objects.isNull(params)){
            throw new CoreException(WSErrors.ERROR_ILLEGAL_PARAMETERS, "Illegal parameters");
        }
        for(Object it : params) {
            if(it == null)
                throw new CoreException(WSErrors.ERROR_ILLEGAL_PARAMETERS, "Illegal parameters");
        }
    }

    public static void checkEqualAny(Object param, Object... values) {
        if(param == null)
            return;
        boolean hit = false;
        if(values == null)
            throw new CoreException(WSErrors.ERROR_ILLEGAL_PARAMETERS, "Illegal parameters");
        if(!hit) {
            for(Object value : values) {
                if(param == value) {
                    hit = true;
                    break;
                }
            }
        }
        if(!hit)
            throw new CoreException(WSErrors.ERROR_ILLEGAL_PARAMETERS, "Illegal parameters");
    }


    public static void checkOneStringNotEmpty(String params){
        if (StringUtils.isEmpty(params)){
            throw new IllegalArgumentException("Illegal params , params is Empty!");
        }
    }


    public static void checkStringAllNotEmpty(String...params){
        if (Objects.isNull(params)){
            throw new IllegalArgumentException("Illegal params , params: " + params);
        }
        for (String it : params) {
            if (StringUtils.isEmpty(it)){
                throw new IllegalArgumentException("Illegal params , params: " + params);
            }
        }
    }

    public static void checkAllNotNullWithString(Object... params) {
        if (Objects.isNull(params)){
            throw new IllegalArgumentException("Illegal params, params：" + params);
        }
        for(Object it : params) {
            if ( Objects.isNull(it) ){
                throw new IllegalArgumentException("Illegal params, params: " + params);
            }else {
                if ( ("String").equals(it.getClass().getSimpleName()) && StringUtils.isEmpty(String.valueOf(it))){
                    throw new IllegalArgumentException("Illegal params, params: " + params);
                }
            }
        }
    }

}
