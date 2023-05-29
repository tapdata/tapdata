package io.tapdata.connector.yashandb;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.kit.EmptyKit;

import java.text.DecimalFormat;

/**
 * Author:Skeet
 * Date: 2023/5/29
 **/
public class YashandbSqlMaker extends CommonSqlMaker {
    private Boolean closeNotNull;

    public YashandbSqlMaker closeNotNull(Boolean closeNotNull) {
        this.closeNotNull = closeNotNull;
        return this;
    }

    protected void buildNullDefinition(StringBuilder builder, TapField tapField) {
        boolean nullable = !(EmptyKit.isNotNull(tapField.getNullable()) && !tapField.getNullable());
        if (closeNotNull && (tapField.getDataType().contains("CHAR") || tapField.getDataType().contains("CLOB"))) {
            nullable = true;
        }
        if (!nullable || tapField.getPrimaryKey()) {
            builder.append("NOT NULL").append(' ');
        }
    }

    public String toTimestampString(DateTime dateTime) {
        StringBuilder sb = new StringBuilder("to_timestamp('" + formatTapDateTime(dateTime, "yyyy-MM-dd HH:mm:ss"));
        int fraction = 0;
        if (dateTime.getNano() > 0) {
            DecimalFormat decimalFormat = new DecimalFormat("000000000");
            String afterZero = decimalFormat.format(dateTime.getNano());
            String realAfterZero = afterZero.replaceAll("(0)+$", "");
            fraction = 9 - afterZero.length() + realAfterZero.length();
            sb.append(".").append(realAfterZero);
        }
        sb.append("', 'yyyy-mm-dd hh24:mi:ss");
        if (fraction > 0) {
            sb.append(".ff").append(fraction);
        }
        sb.append("')");
        return sb.toString();
    }
}
