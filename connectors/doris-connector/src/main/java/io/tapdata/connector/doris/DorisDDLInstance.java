package io.tapdata.connector.doris;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;

import java.util.Collection;
import java.util.LinkedHashMap;

/**
 * @Author dayun
 * @Date 7/14/22
 */
public class DorisDDLInstance {
    private static final DorisDDLInstance DDLInstance = new DorisDDLInstance();

    public static DorisDDLInstance getInstance(){
        return DDLInstance;
    }

    public String buildDistributedKey(Collection<String> primaryKeyNames) {
        StringBuilder builder = new StringBuilder();
        for (String fieldName : primaryKeyNames) {
            builder.append(fieldName);
            builder.append(',');
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    public String buildColumnDefinition(TapTable tapTable) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        StringBuilder builder = new StringBuilder();
        for (String columnName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(columnName);
            if (tapField.getDataType() == null) continue;
            builder.append(tapField.getName()).append(' ');
            builder.append(tapField.getDataType()).append(' ');
            if (tapField.getNullable() != null && !tapField.getNullable()) {
                builder.append("NOT NULL").append(' ');
            } else {
                builder.append("NULL").append(' ');
            }
            if (tapField.getDefaultValue() != null) {
                builder.append("DEFAULT").append(' ').append(tapField.getDefaultValue()).append(' ');
            }
            builder.append(',');
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }}
