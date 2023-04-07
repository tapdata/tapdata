package io.tapdata.connector.selectdb;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * Author:Skeet
 * Date: 2022/12/19
 **/
public class SelectDbDDLInstance {
    private static final SelectDbDDLInstance DDLInstance = new SelectDbDDLInstance();

    public static SelectDbDDLInstance getInstance() {
        return DDLInstance;
    }

    public String buildDistributedKey(Collection<String> primaryKeyNames) {
        StringJoiner joiner = new StringJoiner(",");
        for (String fieldName : primaryKeyNames) {
            joiner.add("`" + fieldName + "`");
        }
        return joiner.toString();
    }

    public String buildColumnDefinition(final TapTable tapTable) {
        final Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        final Collection<String> pks = tapTable.primaryKeys(true);
        Set<String> pkSet = Sets.newHashSet();
        final List<String> fieldStrs = Lists.newArrayList();
        for (String pk : pks) {
            final String fieldStr = concatFieldInCreateSql(nameFieldMap.get(pk));
            fieldStrs.add(fieldStr);
            pkSet.add(pk);
        }
        for (final String columnName : nameFieldMap.keySet()) {
            if (pkSet.contains(columnName)) {
                continue;
            }
            final String fieldStr = concatFieldInCreateSql(nameFieldMap.get(columnName));
            if (StringUtils.isBlank(fieldStr)) {
                continue;
            }
            fieldStrs.add(fieldStr);
        }
        return StringUtils.join(fieldStrs, ",");
    }

    private String concatFieldInCreateSql(final TapField tapField) {
        List<String> fieldStrs = Lists.newArrayList();
        if (tapField.getDataType() == null) {
            return null;
        }
        fieldStrs.add("`" + tapField.getName() + "`");
        fieldStrs.add(tapField.getDataType());
        Boolean nullable = tapField.getNullable();
        if (nullable != null && !nullable) {
            fieldStrs.add("NOT NULL");
        } else {
            fieldStrs.add("NULL");
        }
        if (tapField.getDefaultValue() != null) {
            fieldStrs.add("DEFAULT");
            fieldStrs.add("\"" + tapField.getDefaultValue().toString() + "\"");
        }
        if (tapField.getComment() != null) {
            fieldStrs.add("COMMENT");
            fieldStrs.add(" '" + tapField.getComment() + "'");
        }
        return StringUtils.join(fieldStrs, " ");
    }
}
