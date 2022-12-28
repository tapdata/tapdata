package io.tapdata.connector.selectdb.bean;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;

import java.util.Objects;

/**
 * Author:Skeet
 * Date: 2022/12/12
 **/
public class SelectDbColumn extends CommonColumn {
    public static final String KEY_COLUMN_FLAG = "DUP";
    public static final String KEY_COLUMN_NULLABLE_NO = "NO";
    public static final String COLUMN_KEY_NAME = "COLUMN_NAME";
    public static final String COLUMN_KEY_COLUMN_KEY = "COLUMN_KEY";
    public static final String COLUMN_KEY_COLUMN_TYPE = "COLUMN_TYPE";
    public static final String COLUMN_KEY_IS_NULLABLE = "IS_NULLABLE";
    public static final String COLUMN_KEY_COLUMN_DEFAULT = "COLUMN_DEFAULT";
    public static final String COLUMN_KEY_COLUMN_COMMENT = "COLUMN_COMMENT";

    public static SelectDbColumn create(DataMap dataMap) {
        return new SelectDbColumn(dataMap);
    }

    private boolean isPrimaryKey;
    private String columnKeyType;
    private Object columnDefault;
    private String comment;

    public SelectDbColumn(DataMap dataMap) {
        this.columnName = (String) verifyParamThrow(dataMap, COLUMN_KEY_NAME);
        this.columnKeyType = (String) verifyParamNotThrow(dataMap, COLUMN_KEY_COLUMN_KEY);
        this.isPrimaryKey = Objects.nonNull(columnKeyType) && KEY_COLUMN_FLAG.equals(this.columnKeyType);
        this.columnDefault = verifyParamNotThrow(dataMap, COLUMN_KEY_COLUMN_DEFAULT);
        this.dataType = (String) verifyParamThrow(dataMap, COLUMN_KEY_COLUMN_TYPE);
        this.nullable = (String) verifyParamNotThrow(dataMap, COLUMN_KEY_IS_NULLABLE);
        this.comment = (String) verifyParamNotThrow(dataMap, COLUMN_KEY_COLUMN_COMMENT);
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

    @Override
    public TapField getTapField() {
        TapField tapField = super.getTapField();
        tapField.setName(this.columnName);
        tapField.setPrimaryKey(this.isPrimaryKey);
        tapField.setDataType(this.dataType);
        tapField.setComment(this.comment);
        tapField.setDefaultValue(this.columnDefault);
        tapField.setNullable(!KEY_COLUMN_NULLABLE_NO.equals(this.nullable));
        return tapField;
    }
}