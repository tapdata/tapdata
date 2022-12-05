package io.tapdata.bigquery.util.bigQueryUtil;

import cn.hutool.core.date.DateUtil;
import cn.hutool.json.JSONUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.value.DateTime;

import java.sql.Time;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FieldChecker {
    public static final String FIELD_NAME_REGEX = "^[a-z|A-Z|_]([a-z|A-Z|0-9|_]{0,299})$";
    public static final String ERROR_FIELD_MSG = "Illegal field name [%s],Please use the processor to rename fields according to rules,field name Can only contain letters (a-z, A-Z), numbers (0-9), or underscores (_), Must start with a letter or underscore ,and up to 300 characters.";
    /**
     * column_name 是列的名称。列名称要求：
     *
     * 只能包含字母（a-z、A-Z）、数字 (0-9) 或下划线 (_)
     * 必须以字母或下划线开头
     * 最多包含 300 个字符
     * */
    public static String verifyFieldName(String fieldName){
        if(null == fieldName || !fieldName.matches(FIELD_NAME_REGEX)){
            throw new CoreException(String.format(ERROR_FIELD_MSG,fieldName));
        }
        return fieldName;
    }
    public static void verifyFieldName(Object fieldMap){
        if (null == fieldMap ){
            throw new CoreException("Field map is empty,can not find any field to check.please sure your table is valid.");
        }
        if (fieldMap instanceof Map) {
            ((Map<String,Object>)fieldMap).forEach((key, field) -> FieldChecker.verifyFieldName(key));
        }
    }

    public static String toJsonValue(Object value){
        if (value instanceof Collection && ((Collection)value).isEmpty()) {
            return "NULL";
        }
        if (value instanceof Map && ((Map)value).isEmpty()){
            return "NULL";
        }
        String val = String.valueOf(value);
        return " JSON '" +
                simpleJSONStr(JSONUtil.toJsonStr(val))
                        .replaceAll("'","\\\\'")
                        .replaceAll("\"\\{","\\{")
                        .replaceAll("\"\\[","\\[")
                        .replaceAll("}\"","}")
                        .replaceAll("]\"","]")
//                        .replaceAll("\\\\\"","\"")
                + "'";
    }

    public static String simpleJSONStr(String str){
        if (null == str) return str;
        StringBuilder builder = new StringBuilder();
        boolean skip = true;
        boolean escape = false;
        for (int index = 0; index < str.length(); index++) {
            char ch  = str.charAt(index);
            escape = (!escape && ch == '\\');
            if (skip && (ch==' '|| ch == '\r' || ch == '\n' || ch == '\t')){
                continue;
            }
            builder.append(ch);
            if (ch == '"' && !escape) skip = !skip;
        }
        return replace(builder.toString(),"\r\n","\\\\r\\\\n");

    }

    public static String simpleStringValue(Object value){
        return "'"+String.valueOf(value)
                .replaceAll("'","\\\\'")
                .replaceAll("\"\\{","\\{")
                .replaceAll("\"\\[","\\[")
                .replaceAll("}\"","}")
                .replaceAll("]\"","]")
                +"'";
    }

    public static String simpleValue(Object value){
        return (""+value).replaceAll("'","\\\\'")
                .replaceAll("\"\\{","\\{")
                .replaceAll("\"\\[","\\[")
                .replaceAll("}\"","}")
                .replaceAll("]\"","]")
                ;
    }
    public static String simpleDateValue(Object value,String format){
        if (null == value) return "NULL";
        if (value instanceof DateTime){
            DateTime date = (DateTime) value;
            Date valDate = new Date();
            valDate.setTime(date.getSeconds()*1000);
            return "'"+DateUtil.format(valDate,format)+"'";
        }else if(value instanceof Time || value instanceof Long){
            return "'"+DateUtil.format(new Date(value instanceof Time ? ((Time)value).getTime() : (Long)value ),format)+"'";
        }else if (value instanceof String){
            return (String) value;
        }else {
            return String.valueOf(value);
        }
    }
    public static String simpleYearValue(Object value){
        try {
            if (value instanceof Integer) return ""+value;
            DateTime date = (DateTime) value;
            Date valDate = new Date();
            valDate.setTime(date.getSeconds()*1000);
            return ""+Integer.parseInt(DateUtil.format(valDate,"yyyy"));
        }catch (Exception e){
            TapLogger.debug("FORMAT-YEAR","Can not format the year : {}",value);
            return "NULL";
        }
    }

    public static String replaceNextLine(String str){
        return replace(str,"\r|\n","\\\\n");
    }
    public static String replace(String str,String format,String target){
        if(str!=null && !"".equals(str)) {
            Pattern p = Pattern.compile(format);
            Matcher m = p.matcher(str);
            String strNoBlank = m.replaceAll(target);
            return strNoBlank;
        }else {
            return str;
        }
    }

//    public static void verifyFieldName(Map<String,Object> fieldMap){
//        if (null == fieldMap || fieldMap.isEmpty()){
//            throw new CoreException("Field map is empty,can not find any field to check.please sure your table is valid.");
//        }
//        fieldMap.forEach((key,field)->FieldChecker.verifyFieldName(key));
//    }
}
