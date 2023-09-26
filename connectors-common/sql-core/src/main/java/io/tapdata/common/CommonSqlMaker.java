package io.tapdata.common;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.QueryOperator;
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

    private char escapeChar = '"';

    public CommonSqlMaker() {

    }

    public CommonSqlMaker(char escapeChar) {
        this.escapeChar = escapeChar;
    }

    public char getEscapeChar() {
        return escapeChar;
    }

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
            builder.append(escapeChar).append(tapField.getName()).append(escapeChar).append(' ').append(tapField.getDataType()).append(' ');
            buildDefaultDefinition(builder, tapField);
            buildNullDefinition(builder, tapField);
            if (needComment) {
                buildCommentDefinition(builder, tapField);
            }
            return builder.toString();
        }).collect(Collectors.joining(", "));
    }

    protected void buildNullDefinition(StringBuilder builder, TapField tapField) {
        if ((EmptyKit.isNotNull(tapField.getNullable()) && !tapField.getNullable()) || tapField.getPrimaryKey()) {
            builder.append("NOT NULL").append(' ');
        }
    }

    protected void buildDefaultDefinition(StringBuilder builder, TapField tapField) {
        if (EmptyKit.isNotNull(tapField.getDefaultValue()) && !"".equals(tapField.getDefaultValue())) {
            builder.append("DEFAULT").append(' ');
            if (tapField.getDefaultValue() instanceof Number) {
                builder.append(tapField.getDefaultValue()).append(' ');
            } else {
                builder.append("'").append(tapField.getDefaultValue()).append("' ");
            }
        }
    }

    protected void buildCommentDefinition(StringBuilder builder, TapField tapField) {
        if (EmptyKit.isNotBlank(tapField.getComment())) {
            String comment = tapField.getComment();
            comment = comment.replace("'", "''");
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

    public String buildSqlByAdvanceFilterV2(TapAdvanceFilter filter) {
        StringBuilder builder = new StringBuilder();
        buildRowNumberClause(builder, filter);
        return builder.toString();
    }

    public String buildSelectClause(TapTable tapTable, TapAdvanceFilter filter, boolean needRowNumber) {
        StringBuilder builder = new StringBuilder("SELECT ");
        Projection projection = filter.getProjection();
        if (EmptyKit.isNull(projection) || (EmptyKit.isEmpty(projection.getIncludeFields()) && EmptyKit.isEmpty(projection.getExcludeFields()))) {
            builder.append("*");
        } else {
            builder.append(escapeChar);
            if (EmptyKit.isNotEmpty(filter.getProjection().getIncludeFields())) {
                builder.append(String.join(escapeChar + "," + escapeChar, filter.getProjection().getIncludeFields()));
            } else {
                builder.append(tapTable.getNameFieldMap().keySet().stream()
                        .filter(tapField -> !filter.getProjection().getExcludeFields().contains(tapField)).collect(Collectors.joining(escapeChar + "," + escapeChar)));
            }
            builder.append(escapeChar);
            if (needRowNumber) {
                builder.append(",").append(escapeChar).append("ROWNO_").append(escapeChar);
            }
        }
        builder.append(" FROM ");
        return builder.toString();
    }

    public void buildWhereClause(StringBuilder builder, TapAdvanceFilter filter) {
        if (EmptyKit.isNotEmpty(filter.getMatch()) || EmptyKit.isNotEmpty(filter.getOperators())) {
            builder.append(" WHERE ");
            builder.append(buildKeyAndValue(filter.getMatch(), "AND", "="));
        }
        if (EmptyKit.isNotEmpty(filter.getOperators())) {
            if (EmptyKit.isNotEmpty(filter.getMatch())) {
                builder.append("AND ");
            }
            builder.append(filter.getOperators().stream().map(v -> queryOperatorToString(v, String.valueOf(escapeChar))).collect(Collectors.joining(" AND "))).append(' ');
        }
    }

    public String queryOperatorToString(QueryOperator operator, String quote) {
        String operatorStr;
        switch (operator.getOperator()) {
            case 1:
                operatorStr = ">";
                break;
            case 2:
                operatorStr = ">=";
                break;
            case 3:
                operatorStr = "<";
                break;
            case 4:
                operatorStr = "<=";
                break;
            default:
                operatorStr = "";
        }

        return quote + operator.getKey() + quote + operatorStr + buildValueString(operator.getValue());
    }

    public void buildOrderClause(StringBuilder builder, TapAdvanceFilter filter) {
        if (EmptyKit.isNotEmpty(filter.getSortOnList())) {
            builder.append("ORDER BY ");
            builder.append(filter.getSortOnList().stream().map(v -> v.toString(String.valueOf(escapeChar))).collect(Collectors.joining(", "))).append(' ');
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

    public String buildRowNumberPreClause(TapAdvanceFilter filter) {
        StringBuilder builder = new StringBuilder();
        builder.append("(SELECT A.*,ROW_NUMBER() OVER(");
        if (EmptyKit.isNotEmpty(filter.getSortOnList())) {
            buildOrderClause(builder, filter);
        } else {
            builder.append("ORDER BY 0");
        }
        builder.append(") AS ").append(escapeChar).append("ROWNO_").append(escapeChar).append(" FROM ");
        return builder.toString();
    }

    public void buildRowNumberClause(StringBuilder builder, TapAdvanceFilter filter) {
        builder.append(" A) A ");
        if (EmptyKit.isNotNull(filter.getSkip()) || EmptyKit.isNotNull(filter.getLimit())) {
            builder.append("WHERE ");
        }
        if (EmptyKit.isNotNull(filter.getSkip())) {
            builder.append(escapeChar).append("ROWNO_").append(escapeChar).append(" > ").append(filter.getSkip()).append(' ');
        }
        if (EmptyKit.isNotNull(filter.getLimit())) {
            Integer skip = 0;
            if (EmptyKit.isNotNull(filter.getSkip())) {
                builder.append("AND ");
                skip = filter.getSkip();
            }
            builder.append(escapeChar).append("ROWNO_").append(escapeChar).append(" <= ").append(filter.getLimit() + skip).append(' ');
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
                builder.append(escapeChar).append(fieldName).append(escapeChar).append(operator);
                builder.append(buildValueString(value));
                builder.append(' ').append(splitSymbol).append(' ');
            });
            builder.delete(builder.length() - splitSymbol.length() - 1, builder.length());
        }
        return builder.toString();
    }

    public String buildValueString(Object value) {
        StringBuilder builder = new StringBuilder();
        if (value instanceof Number) {
            builder.append(value);
        } else if (value instanceof DateTime) {
            builder.append(toTimestampString((DateTime) value));
        } else {
            builder.append('\'').append(String.valueOf(value).replaceAll("'", "''")).append('\'');
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
