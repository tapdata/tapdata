package io.tapdata.js.connector.base;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

public class JsUtil {
    public Object toMap(Object obj){
        if (obj instanceof Function){
            Function obj1 = (Function) obj;
            Object apply = obj1.apply(null);
            return apply;
        }else {
            return obj;
        }
    }
    public String nowToDateStr(){
        return this.longToDateStr(System.currentTimeMillis());
    }
    public String nowToDateTimeStr(){
        return this.longToDateTimeStr(System.currentTimeMillis());
    }
    public String longToDateStr(Long date){
        if (null == date) return "1000-01-01";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String dateStr = formatter.format(new Date(date));
        return dateStr.length()>10?"9999-12-31":dateStr;
    }
    public String longToDateTimeStr(Long date){
        if (null == date) return "1000-01-01 00:00:00";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String dateStr = formatter.format(new Date(date));
        return dateStr.length()>19?"9999-12-31 23:59:59":dateStr;
    }
}
