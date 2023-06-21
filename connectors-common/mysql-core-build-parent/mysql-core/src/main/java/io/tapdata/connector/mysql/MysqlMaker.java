package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.entity.MysqlSnapshotOffset;
import io.tapdata.connector.mysql.util.MysqlUtil;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-05-06 20:25
 **/
public class MysqlMaker implements SqlMaker {

    private static final String TAG = MysqlMaker.class.getSimpleName();
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE `%s`.`%s`(\n%s) %s";
    private static final String MYSQL_TABLE_TEMPLATE = "`%s`.`%s`";
    private static final String MYSQL_FIELD_TEMPLATE = "`%s`";
    private static final String MYSQL_ADD_INDEX = "ALTER TABLE `%s`.`%s` ADD %s %s (%s)";
    private boolean hasAutoIncrement;
    protected static final int DEFAULT_CONSTRAINT_NAME_MAX_LENGTH = 30;

    @Override
    public String[] createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent, String version) throws Throwable {
        TapTable tapTable = tapCreateTableEvent.getTable();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        // append field
        String fieldSql = nameFieldMap.values().stream()
                .map(field -> {
                    try {
                        field.setDataType(MysqlUtil.fixDataType(field.getDataType(), version));
                    } catch (Exception e) {
                        TapLogger.warn(TAG, e.getMessage());
                    }
                    return createTableAppendField(field);
                })
                .collect(Collectors.joining(",\n"));
        // primary key
        if (CollectionUtils.isNotEmpty(tapTable.primaryKeys())) {
            fieldSql += ",\n  " + createTableAppendPrimaryKey(tapTable);
        }
        String tablePropertiesSql = "";
        // table comment
        if (StringUtils.isNotBlank(tapTable.getComment())) {
            tablePropertiesSql += " COMMENT='" + tapTable.getComment().replace("'", "''").replaceAll("\\\\", "\\\\\\\\") + "'";
        }

        String sql = String.format(CREATE_TABLE_TEMPLATE, database, tapTable.getId(), fieldSql, tablePropertiesSql);
        return new String[]{sql};
    }

    @Override
    public String selectSql(TapConnectorContext tapConnectorContext, TapTable tapTable, MysqlSnapshotOffset mysqlSnapshotOffset) throws Throwable {
        DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        String tableId = tapTable.getId();
        String sql = String.format(MysqlJdbcContext.SELECT_TABLE, database, tableId);
        Collection<String> pks = tapTable.primaryKeys(true);
        List<String> whereList = new ArrayList<>();
        List<String> orderList = new ArrayList<>();
        if (MapUtils.isNotEmpty(mysqlSnapshotOffset.getOffset())) {
            for (Map.Entry<String, Object> entry : mysqlSnapshotOffset.getOffset().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof Number) {
                    whereList.add(String.format(MysqlJdbcContext.FIELD_TEMPLATE, key) + ">=" + value);
                } else {
                    whereList.add(String.format(MysqlJdbcContext.FIELD_TEMPLATE, key) + ">='" + value + "'");
                }
                orderList.add(String.format(MysqlJdbcContext.FIELD_TEMPLATE, key) + " ASC");
            }
        }
        if (CollectionUtils.isNotEmpty(pks)) {
            for (String pk : pks) {
                String orderStr = String.format(MysqlJdbcContext.FIELD_TEMPLATE, pk) + " ASC";
                if (orderList.contains(orderStr)) {
                    continue;
                }
                orderList.add(orderStr);
            }
        } else {
            TapLogger.info(TAG, "Table {} not support snapshot offset", tapTable.getName());
        }
        if (CollectionUtils.isNotEmpty(whereList)) {
            sql += " WHERE " + String.join(" AND ", whereList);
        }
        if (CollectionUtils.isNotEmpty(orderList)) {
            sql += " ORDER BY " + String.join(",", orderList);
        }
        return sql;
    }

    @Override
    public String selectSql(TapConnectorContext tapConnectorContext, TapTable tapTable, TapAdvanceFilter tapAdvanceFilter) throws Throwable {
        DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        String tableId = tapTable.getId();
        String sql;
		Projection projection = tapAdvanceFilter.getProjection();
        if (EmptyKit.isNull(projection) || (EmptyKit.isEmpty(projection.getIncludeFields()) && EmptyKit.isEmpty(projection.getExcludeFields()))) {
            sql = String.format(MysqlJdbcContext.SELECT_TABLE, database, tableId);
        } else if (EmptyKit.isNotEmpty(tapAdvanceFilter.getProjection().getIncludeFields())) {
			sql = String.format(MysqlJdbcContext.SELECT_SOME_FROM_TABLE, String.join("`,`", tapAdvanceFilter.getProjection().getIncludeFields()), database, tableId);
        } else {
            sql = String.format(MysqlJdbcContext.SELECT_SOME_FROM_TABLE, tapTable.getNameFieldMap().keySet().stream()
                    .filter(tapField -> !tapAdvanceFilter.getProjection().getExcludeFields().contains(tapField)).collect(Collectors.joining("`,`")), database, tableId);
        }
        DataMap match = tapAdvanceFilter.getMatch();
        List<String> whereList = new ArrayList<>();
        if (MapUtils.isNotEmpty(match)) {
            for (Map.Entry<String, Object> entry : match.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof Number) {
                    whereList.add(String.format(MysqlJdbcContext.FIELD_TEMPLATE, key) + "<=>" + value);
                } else if (value instanceof DateTime) {
                    whereList.add(String.format(MysqlJdbcContext.FIELD_TEMPLATE, key) + "<=>'" + dateTimeToStr((DateTime) value, "yyyy-MM-dd HH:mm:ss") + "'");
                } else {
                    whereList.add(String.format(MysqlJdbcContext.FIELD_TEMPLATE, key) + "<=>'" + value + "'");
                }
            }
        }
        List<QueryOperator> operators = tapAdvanceFilter.getOperators();
        if (CollectionUtils.isNotEmpty(operators)) {
            for (QueryOperator operator : operators) {
                String key = operator.getKey();
                Object value = operator.getValue();
                int op = operator.getOperator();
                QueryOperatorEnum queryOperatorEnum = QueryOperatorEnum.fromOp(op);
                String opStr = queryOperatorEnum.getOpStr();
                if (value instanceof Number) {
                    whereList.add(String.format(MysqlJdbcContext.FIELD_TEMPLATE, key) + opStr + value);
                }else if (value instanceof DateTime) {
                    whereList.add(String.format(MysqlJdbcContext.FIELD_TEMPLATE, key) + opStr + "'" + dateTimeToStr((DateTime) value, "yyyy-MM-dd HH:mm:ss") + "'");
                } else {
                    whereList.add(String.format(MysqlJdbcContext.FIELD_TEMPLATE, key) + opStr + "'" + value + "'");
                }
            }
        }
        if (CollectionUtils.isNotEmpty(whereList)) {
            sql += " WHERE " + String.join(" AND ", whereList);
        }
        List<SortOn> sortOnList = tapAdvanceFilter.getSortOnList();
        if (CollectionUtils.isNotEmpty(sortOnList)) {
            List<String> orderList = new ArrayList<>();
            for (SortOn sortOn : sortOnList) {
                String key = sortOn.getKey();
                int sort = sortOn.getSort();
                if (sort == SortOn.ASCENDING) {
                    orderList.add(String.format(MysqlJdbcContext.FIELD_TEMPLATE, key) + " ASC");
                } else if (sort == SortOn.DESCENDING) {
                    orderList.add(String.format(MysqlJdbcContext.FIELD_TEMPLATE, key) + " DESC");
                }
            }
            sql += " ORDER BY " + String.join(",", orderList);
        }
        Integer limit = tapAdvanceFilter.getLimit();
        if (null != limit && limit.compareTo(0) > 0) {
            sql += " LIMIT " + limit;
        }
        Integer skip = tapAdvanceFilter.getSkip();
        if (null != skip && skip.compareTo(0) > 0) {
            sql += " OFFSET " + skip;
        }
        return sql;
    }

    private static String dateTimeToStr(DateTime value, String formatter) {
        return value.toZonedDateTime().format(DateTimeFormatter.ofPattern(formatter));
    }

    @Override
    public String selectSql(TapConnectorContext tapConnectorContext, TapTable tapTable, TapPartitionFilter tapPartitionFilter) throws Throwable {
        String database = tapConnectorContext.getConnectionConfig().getString("database");
        String sql = String.format("SELECT * FROM `%s`.`%s`", database, tapTable.getId());
        if (null == tapPartitionFilter) {
            return sql;
        }
        List<String> whereList = new ArrayList<>();
        QueryOperator leftBoundary = tapPartitionFilter.getLeftBoundary();
        QueryOperator rightBoundary = tapPartitionFilter.getRightBoundary();
        List<QueryOperator> queryOperators = TapSimplify.list(leftBoundary, rightBoundary);
        queryOperators.forEach(o->{
            String queryOperatorSql = getQueryOperatorSql(o);
            if (StringUtils.isNotBlank(queryOperatorSql)) {
                whereList.add(queryOperatorSql);
            }
        });
        DataMap match = tapPartitionFilter.getMatch();
        if (MapUtils.isNotEmpty(match)) {
            match.forEach((k,v)->{
                String valueStr = MysqlUtil.object2String(v);
                whereList.add(String.format("`%s`<=>%s", k, valueStr));
            });
        }
        if (CollectionUtils.isNotEmpty(whereList)) {
            sql += " WHERE " + String.join(" AND ", whereList);
        }
        return sql;
    }

    public String getQueryOperatorSql(QueryOperator queryOperator) {
        String sql;
        if (null == queryOperator) {
            return "";
        }
        int operator = queryOperator.getOperator();
        Object value = queryOperator.getValue();
        String valueStr = MysqlUtil.object2String(value);
        QueryOperatorEnum queryOperatorEnum = QueryOperatorEnum.fromOp(operator);
        sql = "`" + queryOperator.getKey() + "`" + queryOperatorEnum.opStr + valueStr;
        return sql;
    }

    @Override
    public String createIndex(TapConnectorContext tapConnectorContext, TapTable tapTable, TapIndex tapIndex) {
        if (null == tapConnectorContext)
            throw new IllegalArgumentException("Input parameter tapConnectorContext is null");
        if (null == tapTable) throw new IllegalArgumentException("Input parameter tapTable is null");
        if (null == tapIndex) throw new IllegalArgumentException("Input parameter tableIndex is null");
        String database = tapConnectorContext.getConnectionConfig().getString("database");
        if (StringUtils.isBlank(database)) throw new IllegalArgumentException("Database is blank");
        String tableId = tapTable.getId();
        if (StringUtils.isBlank(tableId)) throw new IllegalArgumentException("Table id is blank");
        String indexType;
        if (tapIndex.isUnique()) {
            indexType = "UNIQUE INDEX";
        } else {
            indexType = "INDEX";
        }
        String indexName = StringUtils.isNotBlank(tapIndex.getName()) ? tapIndex.getName() : "";
        List<TapIndexField> indexFields = tapIndex.getIndexFields();
        List<String> fields = indexFields.stream().map(indexField -> {
            String fieldName = indexField.getName();
            Boolean fieldAsc = indexField.getFieldAsc();
            TapField tapField = tapTable.getNameFieldMap().get(fieldName);
            if (null == tapField) {
                throw new RuntimeException(String.format("Index field %s not exists", fieldName));
            }
            String dataType = tapTable.getNameFieldMap().get(fieldName).getDataType();
            String fieldLength = " ";
            if ("text".equalsIgnoreCase(dataType) || "blob".equalsIgnoreCase(dataType)) {
                fieldLength = " (200) ";
            }
            String fieldSort = "ASC";
            if (null != fieldAsc && !fieldAsc) {
                fieldSort = "DESC";
            }
            fieldName = "`" + fieldName + "`" + fieldLength + fieldSort;

            return fieldName;
        }).collect(Collectors.toList());
        return String.format(MYSQL_ADD_INDEX, database, tableId, indexType, indexName, String.join(",", fields));
    }

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
            fieldSql += " NOT NULL";
        } else {
            fieldSql += " NULL";
        }

        // default value
        String defaultValue = tapField.getDefaultValue() == null ? "" : tapField.getDefaultValue().toString();
        if (StringUtils.isNotBlank(defaultValue)) {
            if (defaultValue.contains("'")) {
                defaultValue = StringUtils.replace(defaultValue, "'", "''");
            }
            if (tapField.getTapType() instanceof TapNumber) {
                defaultValue = defaultValue.trim();
            }
            fieldSql += " DEFAULT '" + defaultValue + "'";

        }

        // comment
        String comment = tapField.getComment();
        if (StringUtils.isNotBlank(comment)) {
            // try to escape the single quote in comments
            comment = comment.replace("'", "''").replace("\\", "\\\\");
            fieldSql += " comment '" + comment + "'";
        }

        return fieldSql;
    }

    protected String createTableAppendPrimaryKey(TapTable tapTable) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        String pkSql = "";

        // constraint name
        TapField pkConstraintField = nameFieldMap.values().stream().filter(f -> StringUtils.isNotBlank(f.getConstraint())).findFirst().orElse(null);
        if (pkConstraintField != null) {
            pkSql += "constraint " + getConstraintName(pkConstraintField.getConstraint()) + " primary key (";
        } else {
            pkSql += "primary key (";
        }

        // pk fields
        Collection<String> primaryKeys = tapTable.primaryKeys();
        String pkFieldString = "`" + String.join("`,`", primaryKeys) + "`";

        pkSql += pkFieldString + ")";
        return pkSql;
    }

    protected String getConstraintName(String constraintName) {
        if (StringUtils.isBlank(constraintName)) {
            return "";
        }
        if (constraintName.length() > DEFAULT_CONSTRAINT_NAME_MAX_LENGTH) {
            constraintName = constraintName.substring(0, DEFAULT_CONSTRAINT_NAME_MAX_LENGTH - 4);
        }
        constraintName += RandomStringUtils.randomAlphabetic(4).toUpperCase();
        return constraintName;
    }

    private enum QueryOperatorEnum {
        GT(QueryOperator.GT, ">"), GTE(QueryOperator.GTE, ">="), LE(QueryOperator.LT, "<"), LTE(QueryOperator.LTE, "<="),
        ;

        private int op;
        private String opStr;

        QueryOperatorEnum(int op, String opStr) {
            this.op = op;
            this.opStr = opStr;
        }

        public String getOpStr() {
            return opStr;
        }

        private static Map<String, QueryOperatorEnum> opMap;

        static {
            opMap = new HashMap<>();
            for (QueryOperatorEnum queryOperatorEnum : QueryOperatorEnum.values()) {
                opMap.put(String.valueOf(queryOperatorEnum.op), queryOperatorEnum);
            }
        }

        public static QueryOperatorEnum fromOp(int op) {
            return opMap.get(String.valueOf(op));
        }
    }
}
