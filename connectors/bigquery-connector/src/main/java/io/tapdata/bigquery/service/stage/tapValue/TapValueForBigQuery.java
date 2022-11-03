package io.tapdata.bigquery.service.stage.tapValue;

import cn.hutool.json.JSONUtil;

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
}

