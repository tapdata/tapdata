package io.tapdata.connector.yashandb;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Author:Skeet
 * Date: 2023/5/31
 **/
public class YashandbColumn extends CommonColumn {
    public static final String COLUMN_KEY_NAME = "COLUMN_NAME";
    public static final String COLUMN_KEY_COLUMN_KEY = "COLUMN_KEY";
    public static final String COLUMN_KEY_COLUMN_TYPE = "DATA_TYPE";
    public static final String COLUMN_KEY_IS_NULLABLE = "NULLABLE";
    public static final String COLUMN_KEY_COLUMN_LENGTH = "DATA_LENGTH";
    public static final String COLUMN_KEY_COLUMN_DEFAULT = "DATA_DEFAULT";

    private String columnKeyType;
    private Object columnDefault;
    private int columnLength;

    String[] typeList = {"CHAR", "VARCHAR", "DOUBLE", "FLOAT", "NUMBER", "RAW"};

    public YashandbColumn(DataMap dataMap) {
        this.columnName = (String) verifyParamThrow(dataMap, COLUMN_KEY_NAME);
        this.columnKeyType = (String) verifyParamNotThrow(dataMap, COLUMN_KEY_COLUMN_KEY);
        this.columnDefault = verifyParamNotThrow(dataMap, COLUMN_KEY_COLUMN_DEFAULT);
        this.columnLength = (int) verifyParamThrow(dataMap, COLUMN_KEY_COLUMN_LENGTH);
        this.dataType = isInArray(typeList, (String) verifyParamThrow(dataMap, COLUMN_KEY_COLUMN_TYPE)) ?
                verifyParamThrow(dataMap, COLUMN_KEY_COLUMN_TYPE) + "(" + this.columnLength + ")" :
                (String) verifyParamThrow(dataMap, COLUMN_KEY_COLUMN_TYPE);

        this.nullable = (String) verifyParamNotThrow(dataMap, COLUMN_KEY_IS_NULLABLE);
    }

    public static boolean isInArray(String[] array, String target) {
        for (String element : array) {
            if (element.equals(target)) {
                return true;
            }
        }
        return false;
    }

    private Object verifyParamThrow(DataMap dataMap, String key) {
        Object o = this.verifyParamNotThrow(dataMap, key);
        if (Objects.isNull(o)) {
            throw new CoreException(String.format("Schema column name [%s] must not be null or not be empty. ", key));
        }
        return o;
    }

    private Object verifyParamNotThrow(DataMap dataMap, String key) {
        Object columnValue = dataMap.get(key);
        if (Objects.isNull(columnValue)) {
            return null;
        }
        return columnValue;
    }

    public static YashandbColumn create(DataMap dataMap) {
        return new YashandbColumn(dataMap);
    }

    @Override
    public TapField getTapField() {
        TapField tapField = super.getTapField();
        tapField.setName(this.columnName);
        tapField.setDataType(this.dataType);
        tapField.setDefaultValue(this.columnDefault);
        return tapField;
    }
}
