package io.tapdata.connector.mysql.bean;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;


/**
 * @author jarad
 */
public class MysqlColumn extends CommonColumn {

    public MysqlColumn(DataMap dataMap) {
        this.columnName = dataMap.getString("columnName");
        this.dataType = dataMap.getString("dataType");
        this.nullable = dataMap.getString("nullable");
        this.remarks = dataMap.getString("columnComment");
        this.columnDefaultValue = null;
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

}
