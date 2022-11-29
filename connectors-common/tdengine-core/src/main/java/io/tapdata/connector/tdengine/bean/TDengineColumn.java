package io.tapdata.connector.tdengine.bean;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author IssaacWang
 * @date 2022/10/08
 */
public class TDengineColumn extends CommonColumn {

    private static Set<String> NEED_LENGTH_DATA_TYPE_SET = new HashSet<>();
    static {
        NEED_LENGTH_DATA_TYPE_SET.add("VARCHAR");
        NEED_LENGTH_DATA_TYPE_SET.add("varchar");
        NEED_LENGTH_DATA_TYPE_SET.add("NCHAR");
        NEED_LENGTH_DATA_TYPE_SET.add("nchar");
    }

    public TDengineColumn() {

    }

    public TDengineColumn(DataMap dataMap) {
        this.columnName = dataMap.getString("field");
//        this.dataType = dataMap.getString("type");
        this.dataType = this.buildDataType(dataMap);
//        this.dataType = dataMap.getString("data_type");
//        this.nullable = dataMap.getString("is_nullable");
        this.nullable = "1";
        this.remarks = dataMap.getString("note");
        //create table in target has no need to set default value
        this.columnDefaultValue = null;
    }

    private String buildDataType(DataMap dataMap) {
        Integer length = dataMap.getValue("length", 0);
        String type = dataMap.getString("type");
        return length > 0 && NEED_LENGTH_DATA_TYPE_SET.contains(type) ? String.format("%s(%s)", type, length) : type;
    }

    @Override
    public TapField getTapField() {
        return new TapField(this.columnName, this.dataType).nullable(this.isNullable()).
                defaultValue(columnDefaultValue).comment(this.remarks);
    }

    @Override
    protected Boolean isNullable() {
        return "YES".equals(this.nullable);
    }

    private String getDefaultValue(String defaultValue) {
        if (EmptyKit.isNull(defaultValue) || defaultValue.startsWith("NULL::")) {
            return null;
        } else if (defaultValue.contains("::")) {
            return defaultValue.substring(0, defaultValue.lastIndexOf("::"));
        } else {
            return defaultValue;
        }
    }
}
