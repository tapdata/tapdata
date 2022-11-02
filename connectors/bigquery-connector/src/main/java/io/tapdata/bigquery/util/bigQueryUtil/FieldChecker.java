package io.tapdata.bigquery.util.bigQueryUtil;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapField;

import java.util.LinkedHashMap;
import java.util.Map;

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

//    public static void verifyFieldName(Map<String,Object> fieldMap){
//        if (null == fieldMap || fieldMap.isEmpty()){
//            throw new CoreException("Field map is empty,can not find any field to check.please sure your table is valid.");
//        }
//        fieldMap.forEach((key,field)->FieldChecker.verifyFieldName(key));
//    }
}
