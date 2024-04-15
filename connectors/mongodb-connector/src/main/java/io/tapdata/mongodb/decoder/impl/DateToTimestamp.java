package io.tapdata.mongodb.decoder.impl;

import io.tapdata.entity.error.CoreException;
import io.tapdata.mongodb.decoder.CustomSQLObject;

import java.util.Date;
import java.util.Map;

public class DateToTimestamp implements CustomSQLObject<Object, Map<String, Object>> {
    public static final String DYNAMIC_DATE = "$dateToTimestamp";

    @Override
    public Object execute(Object functionObj, Map<String, Object> curMap) {
        if (functionObj instanceof Date) {
            return ((Date)functionObj).getTime();
        }
        if (null != functionObj) {
            throw new CoreException("{} cannot resolve non type {} objects to timestamp", DYNAMIC_DATE, functionObj.getClass());
        }
        return null;
    }

    @Override
    public String getFunctionName() {
        return DYNAMIC_DATE;
    }
}
