package io.tapdata.oceanbase;

import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.oceanbase.util.QueryOperatorEnum;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author dayun
 * @date 2022/6/23 17:00
 */
public class OceanbaseMaker {
    private static final String TAG = OceanbaseMaker.class.getSimpleName();
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE `%s`.`%s`(\n%s) %s";
    private static final String MYSQL_TABLE_TEMPLATE = "`%s`.`%s`";
    private static final String MYSQL_FIELD_TEMPLATE = "`%s`";
    private static final String MYSQL_ADD_INDEX = "ALTER TABLE `%s`.`%s` ADD %s %s (%s)";
    private static final String SELECT_TABLE = "SELECT t.* FROM `%s`.`%s` t";
    private boolean hasAutoIncrement;
    protected static final int DEFAULT_CONSTRAINT_NAME_MAX_LENGTH = 30;

    public static String[] createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        // append field
        String fieldSql = nameFieldMap.values().stream().map(OceanbaseMaker::createTableAppendField).collect(Collectors.joining(",\n"));
        // primary key
        if (CollectionUtils.isNotEmpty(tapTable.primaryKeys())) {
            fieldSql += ",\n  " + createTableAppendPrimaryKey(tapTable);
        }
        String tablePropertiesSql = "";
        // table comment
        if (StringUtils.isNotBlank(tapTable.getComment())) {
            tablePropertiesSql += " COMMENT='" + tapTable.getComment() + "'";
        }

        String sql = String.format(CREATE_TABLE_TEMPLATE, database, tapTable.getId(), fieldSql, tablePropertiesSql);
        return new String[]{sql};
    }

    private static String createTableAppendField(TapField tapField) {
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
		/*String defaultValue = tapField.getDefaultValue() == null ? "" : tapField.getDefaultValue().toString();
		if (StringUtils.isNotBlank(defaultValue)) {
			fieldSql += " DEFAULT '" + defaultValue + "'";
		}*/

        // comment
        String comment = tapField.getComment();
        if (StringUtils.isNotBlank(comment)) {
            // try to escape the single quote in comments
            comment = comment.replace("'", "''");
            fieldSql += " comment '" + comment + "'";
        }

        return fieldSql;
    }

    private static String createTableAppendPrimaryKey(TapTable tapTable) {
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

    private static String getConstraintName(String constraintName) {
        if (StringUtils.isBlank(constraintName)) {
            return "";
        }
        if (constraintName.length() > DEFAULT_CONSTRAINT_NAME_MAX_LENGTH) {
            constraintName = constraintName.substring(0, DEFAULT_CONSTRAINT_NAME_MAX_LENGTH - 4);
        }
        constraintName += RandomStringUtils.randomAlphabetic(4).toUpperCase();
        return constraintName;
    }

    public static String selectSql(TapConnectorContext tapConnectorContext, TapTable tapTable, TapAdvanceFilter tapAdvanceFilter) {
        DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        String tableId = tapTable.getId();
        String sql = String.format(SELECT_TABLE, database, tableId);

        DataMap match = tapAdvanceFilter.getMatch();
        List<String> whereList = new ArrayList<>();
        if (MapUtils.isNotEmpty(match)) {
            for (Map.Entry<String, Object> entry : match.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof Number) {
                    whereList.add(key + "<=>" + value);
                } else {
                    whereList.add(key + "<=>'" + value + "'");
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
                    whereList.add(key + opStr + value);
                } else {
                    whereList.add(key + opStr + "'" + value + "'");
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
                    orderList.add(key + " ASC");
                } else if (sort == SortOn.DESCENDING) {
                    orderList.add(key + " DESC");
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
}
