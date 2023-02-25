package io.tapdata.bigquery.service.stage.tapvalue;

import cn.hutool.core.date.DateUtil;
import cn.hutool.json.JSONUtil;
import io.tapdata.entity.logger.TapLogger;

public interface TapValueForBigQuery {
    public default String value(Object value){
        return toJsonValue(value);
    }

    public static String toJsonValue(Object value){
        String val = String.valueOf(value);
        return " JSON '" +
                JSONUtil.toJsonPrettyStr(val)
                        .replaceAll("'","\\'")
                        .replaceAll("\"\\{","\\{")
                        .replaceAll("}\"","}")
                        .replaceAll("\n","")
                + "'";
    }

    public static String simpleStringValue(Object value){
        return "'"+String.valueOf(value).replaceAll("'","\\'")+"'";
    }

    public static String simpleValue(Object value){
        return ""+value;
    }
    public static String simpleDateValue(Object value,String format){
        String val = String.valueOf(value);
        try {
            return DateUtil.format(DateUtil.parseDate(val),format);
        }catch (Exception e){
            TapLogger.debug("","Can not format the time : {}",value);
            return "NULL";
        }
    }
}

