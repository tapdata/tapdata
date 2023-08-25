package io.tapdata.connector.mysql.bean;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;


/**
 * @author jarad
 */
public class MysqlColumn extends CommonColumn {

    private String version;

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

    public MysqlColumn withVersion(String version) {
        this.version = version;
        return this;
    }

    @Override
    protected Boolean isNullable() {
        if (EmptyKit.isNull(version) || "5.6".compareTo(version) > 0) {
            return true;
        }
        return "YES".equals(this.nullable);
    }

}
