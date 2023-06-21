package io.tapdata.connector.hive1.ddl.impl;

import io.tapdata.connector.hive1.ddl.DDLSqlMaker;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Hive1JDBCSqlMaker implements DDLSqlMaker {


    public static String buildColumnDefinition(TapTable tapTable, boolean needComment) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        //no primary key,need judge logic primary key
//        Collection<String> primaryKeys = tapTable.primaryKeys(true);
//        Collection<String> logicPrimaryKeys = EmptyKit.isNotEmpty(primaryKeys) ? Collections.emptyList() : tapTable.primaryKeys(true);
        return nameFieldMap.entrySet().stream().sorted(Comparator.comparing(v ->
                EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).map(v -> { //pos may be null
            StringBuilder builder = new StringBuilder();
            TapField tapField = v.getValue();
            //ignore those which has no dataType
            if (tapField.getDataType() == null) {
                return "";
            }
            tapField.setDataType(tapField.getDataType().replace("unsigned", "").replace("UNSIGNED", ""));
            builder.append('`').append(tapField.getName()).append("` ");

            builder.append(tapField.getDataType()).append(' ');
            Object defaultValue = tapField.getDefaultValue();
            if (defaultValue != null && StringUtils.isNotBlank((defaultValue.toString())) && !"null".equalsIgnoreCase(defaultValue.toString())) {
                builder.append("DEFAULT").append(' ');
                if (tapField.getDefaultValue() instanceof Number) {
                    builder.append(defaultValue).append(' ');
                } else {
                    builder.append("'").append(defaultValue).append("' ");
                }
            }
            if (needComment && EmptyKit.isNotBlank(tapField.getComment())) {
                String comment = tapField.getComment();
                comment = comment.replace("'", "''");
                builder.append("comment '").append(comment).append("' ");
            }
            return builder.toString();
        }).collect(Collectors.joining(", "));
    }

    /**
     * build subSql after where for advance query
     *
     * @param filter condition of advance query
     * @return where substring
     */
    public static String buildSqlByAdvanceFilter(TapAdvanceFilter filter) {
        StringBuilder builder = new StringBuilder();
        if (EmptyKit.isNotEmpty(filter.getMatch()) || EmptyKit.isNotEmpty(filter.getOperators())) {
            builder.append("WHERE ");
            builder.append(buildKeyAndValue(filter.getMatch(), "AND", "="));
        }
        if (EmptyKit.isNotEmpty(filter.getOperators())) {
            if (EmptyKit.isNotEmpty(filter.getMatch())) {
                builder.append("AND ");
            }
            builder.append(filter.getOperators().stream().map(v -> v.toString("\"")).collect(Collectors.joining(" AND "))).append(' ');
        }
        if (EmptyKit.isNotEmpty(filter.getSortOnList())) {
            builder.append("ORDER BY ");
            builder.append(filter.getSortOnList().stream().map(v -> v.toString("\"")).collect(Collectors.joining(", "))).append(' ');
        }
        if (null != filter.getLimit()) {
            builder.append("LIMIT ").append(filter.getLimit()).append(' ');
        }
        if (null != filter.getSkip()) {
            builder.append("OFFSET ").append(filter.getSkip()).append(' ');
        }

        return builder.toString();
    }

    /**
     * set value for each column in sql
     * e.g.
     * id=12,name=Jarad,age=34
     *
     * @param record      key-val
     * @param splitSymbol split symbol
     * @return substring of sql
     */
    public static String buildKeyAndValue(Map<String, Object> record, String splitSymbol, String operator) {
        StringBuilder builder = new StringBuilder();
        if (EmptyKit.isNotEmpty(record)) {
            record.forEach((fieldName, value) -> {
                builder.append(fieldName).append(operator);
                if (!(value instanceof Number)) {
                    builder.append('\'').append(value).append('\'');
                } else {
                    builder.append(value);
                }
                builder.append(' ').append(splitSymbol).append(' ');
            });
            builder.delete(builder.length() - splitSymbol.length() - 1, builder.length());
        }
        return builder.toString();
    }
}
