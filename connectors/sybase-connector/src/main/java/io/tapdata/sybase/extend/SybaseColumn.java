package io.tapdata.sybase.extend;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;

/**
 * @author GavinXiao
 * @description SybaseColumn create by Gavin
 * @create 2023/7/11 19:37
 **/
public class SybaseColumn extends CommonColumn {
    public SybaseColumn(DataMap dataMap) {
        this.columnName = dataMap.getString("columnName");
        this.dataType = dataMap.getString("dataType");
        this.nullable = dataMap.getString("nullable");
        this.remarks = dataMap.getString("columnComment");
        this.columnDefaultValue = null;
    }

    public SybaseColumn() {

    }

    @Override
    public TapField getTapField() {
        return new TapField(this.columnName, this.dataType).nullable(this.isNullable()).
                defaultValue(columnDefaultValue).comment(this.remarks);
    }

    @Override
    protected Boolean isNullable() {
        return "NULL".equals(this.nullable);
    }

    public TapField initTapField(DataMap dataMap) {
        return new TapField(dataMap.getString("columnName"), dataMap.getString("dataType"))
                .nullable("NULL".equals(dataMap.getString("nullable")))
                .defaultValue(null)
                .comment(dataMap.getString("columnComment"));
    }
}
