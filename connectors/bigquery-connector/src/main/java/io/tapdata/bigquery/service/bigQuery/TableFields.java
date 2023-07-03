package io.tapdata.bigquery.service.bigQuery;

import io.tapdata.bigquery.util.tool.Checker;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapNumber;

public class TableFields {
    private static final String TAG = TableFields.class.getSimpleName();

    protected String createTableAppendField(TapField tapField) {
        String datatype = tapField.getDataType().toUpperCase();
        String fieldSql = "  `" + tapField.getName() + "`" + " " + tapField.getDataType().toUpperCase();

        // auto increment
        // mysql a table can only create one auto-increment column, and must be the primary key
        if (null != tapField.getAutoInc() && tapField.getAutoInc()) {
            if (tapField.getPrimaryKeyPos() == 1) {
                fieldSql += " AUTO_INCREMENT";
            } else {
                TapLogger.warn(TAG, "Field \"{}\" cannot be auto increment in mysql, there can be only one auto column and it must be defined the first key", tapField.getName());
            }
        }

        // nullable
        if ((null != tapField.getNullable() && !tapField.getNullable()) || (null != tapField.getPrimaryKeyPos() && tapField.getPrimaryKeyPos() > 0)) {
            fieldSql += " NULLABLE ";
        } else {
            fieldSql += " REPEATED ";
        }

        // default value
        String defaultValue = tapField.getDefaultValue() == null ? "" : tapField.getDefaultValue().toString();
        if (Checker.isNotEmpty(defaultValue)) {
            defaultValue = defaultValue.replace("'", "''");
            if (tapField.getTapType() instanceof TapNumber) {
                defaultValue = defaultValue.trim();
            }
            fieldSql += " DEFAULT '" + defaultValue + "'";
        }

        // comment
        String comment = tapField.getComment();
        if (Checker.isNotEmpty(comment)) {
            // try to escape the single quote in comments
            comment = comment.replace("'", "''");
            fieldSql += " comment '" + comment + "'";
        }

        return fieldSql;
    }
}
