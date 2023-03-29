package io.tapdata.common;

import io.tapdata.entity.schema.TapField;

/**
 * attributes for common columns
 *
 * @author Jarad
 * @date 2022/4/20
 */
public class CommonColumn {

    protected String columnName;
    protected String dataType;
    protected String nullable;
    protected String remarks;
    protected String columnDefaultValue;
    protected String autoInc;

    public CommonColumn() {
    }

    protected Boolean isNullable() {
        return "1".equals(this.nullable);
    }

    protected Boolean isAutoInc() {
        return "1".equals(this.autoInc);
    }

    public TapField getTapField() {
        return new TapField(this.columnName, this.dataType).nullable(this.isNullable()).
                defaultValue(columnDefaultValue).comment(this.remarks);
    }
}
