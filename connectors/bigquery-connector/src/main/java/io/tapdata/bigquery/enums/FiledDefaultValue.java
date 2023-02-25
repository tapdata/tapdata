package io.tapdata.bigquery.enums;

public enum FiledDefaultValue {
        CURRENT_DATE("","CURRENT_DATE()"),
        CURRENT_DATETIME("","CURRENT_DATETIME()"),
        CURRENT_TIME("","CURRENT_TIME()"),
        CURRENT_TIMESTAMP("","CURRENT_TIMESTAMP()"),
        GENERATE_UUID("","GENERATE_UUID()"),
        RAND("","RAND()"),
        SESSION_USER("","SESSION_USER()"),
        ST_GEOGPOINT("","ST_GEOGPOINT()")

    ;
        String fieldName;
        String defaultValueName;
    FiledDefaultValue(String fieldName,String defaultValueName){
        this.defaultValueName = defaultValueName;
        this.fieldName = fieldName;
    }
    public static String defaultValueFun(String fieldName){
        FiledDefaultValue filedDefaultValue = FiledDefaultValue.defaultFunByFieldName(fieldName);
        if (null == filedDefaultValue) return null;
        return filedDefaultValue.defaultValueName;
    }
    public static FiledDefaultValue defaultFunByFieldName(String fieldName){
        if (null == fieldName || "".equals(fieldName)) return null;
        FiledDefaultValue[] values = values();
        for (FiledDefaultValue value : values) {
            if (value.fieldName.equals(fieldName)) return value;
        }
        return null;
    }

    public String getDefaultValueName() {
        return defaultValueName;
    }

    public void setDefaultValueName(String defaultValueName) {
        this.defaultValueName = defaultValueName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
}
