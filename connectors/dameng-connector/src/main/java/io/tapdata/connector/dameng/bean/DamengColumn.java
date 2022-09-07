package io.tapdata.connector.dameng.bean;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;


/**
 * @author lemon
 */
public class DamengColumn extends CommonColumn {

    public DamengColumn(DataMap dataMap) {
        this.columnName = dataMap.getString("COLUMN_NAME");
        this.dataType = getDataType(dataMap);
        this.nullable = dataMap.getString("NULLABLE");
        this.remarks = dataMap.getString("COMMENTS");
        this.columnDefaultValue = null;
    }

    @Override
    public TapField getTapField() {
        return new TapField(this.columnName, this.dataType).nullable(this.isNullable()).
                defaultValue(columnDefaultValue).comment(this.remarks);
    }

    @Override
    protected Boolean isNullable() {
        return "Y".equals(this.nullable);
    }

    private String getDataType(DataMap dataMap) {
        String dataType = dataMap.getString("DATA_TYPE");
        String dataLength = dataMap.getString("DATA_LENGTH");
        String dataPrecision = dataMap.getString("DATA_PRECISION");
        String dataScale = dataMap.getString("DATA_SCALE");
        if (dataType.contains("(")) {
            return dataType;
        } else {
            switch (dataType) {
                case "CHAR":
                case "VARCHAR":
                case "TEXT":
                    return dataType + "(" + dataLength + ")";
                case "NUMBER":
                    if (EmptyKit.isNull(dataPrecision) && EmptyKit.isNull(dataScale)) {
                        return "NUMBER";
                    } else if (EmptyKit.isNull(dataPrecision)) {
                        return "NUMBER(*," + dataScale + ")";
                    } else {
                        return "NUMBER(" + dataPrecision + "," + dataScale + ")";
                    }
                default:
                    return dataType;
            }
        }
    }
}
