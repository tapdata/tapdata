package io.tapdata.common;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * make sql
 *
 * @author Jarad
 * @date 2022/4/29
 */
public class CommonSqlMaker {

    /**
     * combine column definition for creating table
     * e.g.
     * id text ,
     * tapString text NOT NULL ,
     * tddUser text ,
     * tapString10 VARCHAR(10) NOT NULL
     *
     * @param tapTable Table Object
     * @return substring of SQL
     */
    public String buildColumnDefinition(TapTable tapTable, boolean needComment) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        return nameFieldMap.entrySet().stream().sorted(Comparator.comparing(v ->
                EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).map(v -> { //pos may be null
            StringBuilder builder = new StringBuilder();
            TapField tapField = v.getValue();
            //ignore those which has no dataType
            if (tapField.getDataType() == null) {
                return "";
            }
            builder.append('\"').append(tapField.getName()).append("\" ").append(tapField.getDataType()).append(' ');
            buildDefaultDefinition(builder, tapField);
            buildNullDefinition(builder, tapField);
            if (needComment) {
                buildCommentDefinition(builder, tapField);
            }
            return builder.toString();
        }).collect(Collectors.joining(", "));
    }

    private void buildNullDefinition(StringBuilder builder, TapField tapField) {
        if ((EmptyKit.isNotNull(tapField.getNullable()) && !tapField.getNullable()) || tapField.getPrimaryKey()) {
            builder.append("NOT NULL").append(' ');
        }
    }

    private void buildDefaultDefinition(StringBuilder builder, TapField tapField) {
        if (EmptyKit.isNotNull(tapField.getDefaultValue()) && !"".equals(tapField.getDefaultValue())) {
            builder.append("DEFAULT").append(' ');
            if (tapField.getDefaultValue() instanceof Number) {
                builder.append(tapField.getDefaultValue()).append(' ');
            } else {
                builder.append("'").append(tapField.getDefaultValue()).append("' ");
            }
        }
    }

    private void buildCommentDefinition(StringBuilder builder, TapField tapField) {
        if (EmptyKit.isNotBlank(tapField.getComment())) {
            String comment = tapField.getComment();
            comment = comment.replace("'", "\\'");
            builder.append("comment '").append(comment).append("' ");
        }
    }

    /**
     * build subSql after where for advance query
     *
     * @param filter condition of advance query
     * @return where substring
     */
    public String buildSqlByAdvanceFilter(TapAdvanceFilter filter) {
        StringBuilder builder = new StringBuilder();
        buildWhereClause(builder, filter);
        buildOrderClause(builder, filter);
        buildLimitOffsetClause(builder, filter);
        return builder.toString();
    }

    public void buildWhereClause(StringBuilder builder, TapAdvanceFilter filter) {
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
    }

    public void buildOrderClause(StringBuilder builder, TapAdvanceFilter filter) {
        if (EmptyKit.isNotEmpty(filter.getSortOnList())) {
            builder.append("ORDER BY ");
            builder.append(filter.getSortOnList().stream().map(v -> v.toString("\"")).collect(Collectors.joining(", "))).append(' ');
        }
    }

    public void buildLimitOffsetClause(StringBuilder builder, TapAdvanceFilter filter) {
        if (EmptyKit.isNotNull(filter.getLimit())) {
            builder.append("LIMIT ").append(filter.getLimit()).append(' ');
        }
        if (EmptyKit.isNotNull(filter.getSkip())) {
            builder.append("OFFSET ").append(filter.getSkip()).append(' ');
        }
    }

    public void buildRowNumberClause(StringBuilder builder, TapAdvanceFilter filter) {
        builder.append(") ");
        if (EmptyKit.isNotNull(filter.getSkip()) || EmptyKit.isNotNull(filter.getLimit())) {
            builder.append("WHERE ");
        }
        if (EmptyKit.isNotNull(filter.getSkip())) {
            builder.append("ROWNUM > ").append(filter.getSkip()).append(' ');
        }
        if (EmptyKit.isNotNull(filter.getLimit())) {
            Integer skip = 0;
            if (EmptyKit.isNotNull(filter.getSkip())) {
                builder.append("AND ");
                skip = filter.getSkip();
            }
            builder.append("ROWNUM <= ").append(filter.getLimit() + skip).append(' ');
        }
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
    public String buildKeyAndValue(Map<String, Object> record, String splitSymbol, String operator) {
        StringBuilder builder = new StringBuilder();
        if (EmptyKit.isNotEmpty(record)) {
            record.forEach((fieldName, value) -> {
                builder.append('\"').append(fieldName).append('\"').append(operator);
                if (value instanceof Number) {
                    builder.append(value);
                } else if (value instanceof DateTime) {
                    builder.append(toTimestampString((DateTime) value));
                } else {
                    builder.append('\'').append(value).append('\'');
                }
                builder.append(' ').append(splitSymbol).append(' ');
            });
            builder.delete(builder.length() - splitSymbol.length() - 1, builder.length());
        }
        return builder.toString();
    }

    public String toTimestampString(DateTime dateTime) {
        StringBuilder sb = new StringBuilder("'" + formatTapDateTime(dateTime, "yyyy-MM-dd HH:mm:ss"));
        if (dateTime.getNano() > 0) {
            DecimalFormat decimalFormat = new DecimalFormat("000000000");
            sb.append(".").append(decimalFormat.format(dateTime.getNano()).replaceAll("(0)+$", ""));
        }
        sb.append('\'');
        return sb.toString();
    }

    public String formatTapDateTime(DateTime dateTime, String pattern) {
        try {
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);
            final ZoneId zoneId = dateTime.getTimeZone() != null ? dateTime.getTimeZone().toZoneId() : ZoneId.of("GMT");
            LocalDateTime localDateTime = LocalDateTime.ofInstant(dateTime.toInstant(), zoneId);
            return dateTimeFormatter.format(localDateTime);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * order by used in batchRead offset
     *
     * @param tapTable table
     * @return order by clause
     */
    public String getOrderByUniqueKey(TapTable tapTable) {
        StringBuilder orderBy = new StringBuilder();
        orderBy.append(" ORDER BY ");
        List<TapIndex> indexList = tapTable.getIndexList();
        //has no indexes, need each field
        if (EmptyKit.isEmpty(indexList)) {
            orderBy.append(tapTable.getNameFieldMap().keySet().stream().map(field -> "\"" + field + "\"")
                    .reduce((v1, v2) -> v1 + ", " + v2).orElseGet(String::new));
        }
        //has indexes but no unique
        else if (indexList.stream().noneMatch(TapIndex::isUnique)) {
            TapIndex index = indexList.stream().findFirst().orElseGet(TapIndex::new);
            orderBy.append(index.getIndexFields().stream().map(field -> "\"" + field.getName() + "\" " + (field.getFieldAsc() ? "ASC" : "DESC"))
                    .reduce((v1, v2) -> v1 + ", " + v2).orElseGet(String::new));
            List<String> indexFields = index.getIndexFields().stream().map(TapIndexField::getName).collect(Collectors.toList());
            if (tapTable.getNameFieldMap().size() > indexFields.size()) {
                orderBy.append(',');
                orderBy.append(tapTable.getNameFieldMap().keySet().stream().filter(key -> !indexFields.contains(key)).map(field -> "\"" + field + "\"")
                        .reduce((v1, v2) -> v1 + ", " + v2).orElseGet(String::new));
            }
        }
        //has unique indexes
        else {
            TapIndex uniqueIndex = indexList.stream().filter(TapIndex::isUnique).findFirst().orElseGet(TapIndex::new);
            orderBy.append(uniqueIndex.getIndexFields().stream().map(field -> "\"" + field.getName() + "\" " + (field.getFieldAsc() ? "ASC" : "DESC"))
                    .reduce((v1, v2) -> v1 + ", " + v2).orElseGet(String::new));
        }
        return orderBy.toString();
    }

}
