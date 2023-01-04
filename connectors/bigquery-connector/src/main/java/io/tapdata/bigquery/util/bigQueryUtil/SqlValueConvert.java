package io.tapdata.bigquery.util.bigQueryUtil;

import cn.hutool.core.codec.Base64;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapType;

public class SqlValueConvert {
    /**
     * JSON :  INSERT INTO mydataset.table1 VALUES(1, JSON '{"name": "Alice", "age": 30}');
     *
     * */
    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    final static String DATE_FORMAT = "yyyy-MM-dd";
    final static String TIME_FORMAT = "HH:mm:ss";
    final static String YEAR_FORMAT = "yyyy-MM-dd";
    public static String sqlValue(Object value, TapField field){
        TapType tapType = field.getTapType();
        switch(tapType.getClass().getSimpleName()){
            case "TapString" : return FieldChecker.simpleStringValue(value);
            case "TapArray": return FieldChecker.toJsonValue(value);
            case "TapBinary": return null == value ? "NULL":" FROM_BASE64('"+ Base64.encode(String.valueOf(value)) +"') ";
            case "TapBoolean": return FieldChecker.simpleValue(value);
            case "TapDateTime": return FieldChecker.simpleDateValue(value,DATE_TIME_FORMAT);
            case "TapDate": return FieldChecker.simpleDateValue(value,DATE_FORMAT);
            case "TapTime": return FieldChecker.simpleDateValue(value,TIME_FORMAT);
            case "TapMap": return FieldChecker.toJsonValue(value);
            case "TapNumber": return FieldChecker.simpleValue(value);
            case "TapYear": return FieldChecker.simpleYearValue(value);
            default:return ""+value;
        }
    }
}
