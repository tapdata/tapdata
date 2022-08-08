package io.tapdata.connector.doris.bean;

import io.tapdata.entity.schema.TapField;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author dayun
 * @Date 7/14/22
 */
public class DorisColumn {
    // ref https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html
    private String tableCatalog;
    private String tableSchema;
    private String tableName;
    private String columnName;
    private String dataType;
    private String typeName;
    private String columnSize;
    private String decimalDigits;
    private String numPrecisionRadix;
    private String nullable;
    private String remarks;
    private String columnDefaultValue;
    private String charOctetLength;
    private String ordinalPosition;
    private String sourceDataType;
    private String isAutoincrement;

    private DorisColumn() {
    }

    public DorisColumn(ResultSet resultSet) throws SQLException {
        this.tableCatalog = resultSet.getString("TABLE_CAT");
        this.tableSchema = resultSet.getString("TABLE_SCHEM");
        this.tableName = resultSet.getString("TABLE_NAME");
        this.columnName = resultSet.getString("COLUMN_NAME");
        this.dataType = resultSet.getString("DATA_TYPE");
        this.typeName = resultSet.getString("TYPE_NAME");
        this.columnSize = resultSet.getString("COLUMN_SIZE");

        this.decimalDigits = resultSet.getString("DECIMAL_DIGITS");
        this.numPrecisionRadix = resultSet.getString("NUM_PREC_RADIX");
        this.nullable = resultSet.getString("NULLABLE");
        this.remarks = resultSet.getString("REMARKS");
        this.columnDefaultValue = resultSet.getString("COLUMN_DEF");

        this.charOctetLength = resultSet.getString("CHAR_OCTET_LENGTH");
        this.ordinalPosition = resultSet.getString("ORDINAL_POSITION");

        this.sourceDataType = resultSet.getString("SOURCE_DATA_TYPE");
        this.isAutoincrement = resultSet.getString("IS_AUTOINCREMENT");
    }


    private Boolean isNullable() {
        return "1".equals(this.nullable);
    }


    public TapField getTapField() {
        return new TapField(this.columnName, this.dataType).nullable(this.isNullable()).
                defaultValue(columnDefaultValue).comment(this.remarks);
    }
}
