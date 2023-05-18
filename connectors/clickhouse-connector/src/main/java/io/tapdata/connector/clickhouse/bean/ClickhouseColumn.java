package io.tapdata.connector.clickhouse.bean;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;

public class ClickhouseColumn extends CommonColumn {

    private final boolean isPartition;
    private final boolean isPrimary;
    private final boolean isSorting;

    public ClickhouseColumn(DataMap dataMap) {
        this.columnName = dataMap.getString("columnName");
        String columnType = dataMap.getString("dataType");
        if (columnType.contains("Nullable")) {
            columnType = columnType.replace("Nullable(", "");
            this.dataType = columnType.substring(0, columnType.length() - 1);
            this.nullable = "1";
        } else {
            this.dataType = columnType;
        }
        this.isPartition = "1".equals(dataMap.getString("isPartition"));
        this.isPrimary = "1".equals(dataMap.getString("isPk"));
        this.isSorting = "1".equals(dataMap.getString("isSorting"));
        this.remarks = dataMap.getString("columnComment");
        this.columnDefaultValue = null;
    }

    protected Boolean isNullable() {
        return "1".equals(this.nullable);
    }

    protected Boolean isAutoInc() {
        return "1".equals(this.autoInc);
    }

    public TapField getTapField() {
        return new TapField(this.columnName, this.dataType)
                .nullable(this.isNullable())
                .defaultValue(columnDefaultValue)
                .comment(this.remarks)
//                .isPartitionKey(isPartition)
                .isPrimaryKey(isPrimary);
    }

    public boolean isPartition() {
        return isPartition;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public boolean isSorting() {
        return isSorting;
    }
}
