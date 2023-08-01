package io.tapdata.connector.postgres;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.entity.schema.TapField;
import io.tapdata.kit.EmptyKit;

public class PostgresSqlMaker extends CommonSqlMaker {

    private Boolean closeNotNull;

    public PostgresSqlMaker closeNotNull(Boolean closeNotNull) {
        this.closeNotNull = closeNotNull;
        return this;
    }

    protected void buildNullDefinition(StringBuilder builder, TapField tapField) {
        boolean nullable = !(EmptyKit.isNotNull(tapField.getNullable()) && !tapField.getNullable());
        if (closeNotNull) {
            nullable = true;
        }
        if (!nullable || tapField.getPrimaryKey()) {
            builder.append("NOT NULL").append(' ');
        }
    }

}
