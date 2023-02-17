package io.tapdata.connector.guass;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;

public class GuassColumn extends CommonColumn {


    public GuassColumn() {

    }

    public GuassColumn(DataMap dataMap) {
        this.columnName = dataMap.getString("column_name");
        this.dataType = dataMap.getString("dataType"); //'dataType' with precision and scale (postgres has its function)
//        this.dataType = dataMap.getString("data_type"); //'data_type' without precision or scale
        this.nullable = dataMap.getString("is_nullable");
        this.remarks = dataMap.getString("remark");
        //create table in target has no need to set default value
        this.columnDefaultValue = null;
//        this.columnDefaultValue = getDefaultValue(dataMap.getString("column_default"));
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


