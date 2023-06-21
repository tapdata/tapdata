package io.tapdata.connector.tidb.ddl;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.ddl.DDLSqlMaker;
import io.tapdata.connector.mysql.MysqlMaker;
import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lemon
 */
public class TidbSqlMaker extends MysqlMaker implements DDLSqlMaker {
    private static final String TAG = TidbSqlMaker.class.getSimpleName();
    private static final String TIDB_TABLE_COMMENT_TEMPLATE = "exec sp_addextendedproperty 'TIDB_Description', '%s', 'SCHEMA', '%s', 'TABLE', '%s'";
    private static final String TIDB_FIELD_COMMENT_TEMPLATE = "exec sp_addextendedproperty 'TIDB_Description', '%s', 'SCHEMA', '%s', 'TABLE', '%s', 'COLUMN', '%s'";

    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE `%s`.`%s`(\n%s) %s";

    private final static String ALTER_TABLE_PREFIX = "alter table `%s`.`%s`";

    protected static final int DEFAULT_CONSTRAINT_NAME_MAX_LENGTH = 30;

    private static final String TIDB_OBJECT_EXISTENCE_NULL_TEMPLATE = "IF OBJECT_ID('%s', 'U') IS NULL \n";

    public String createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        // append field
        String fieldSql = nameFieldMap.values().stream()
                .map(field -> {
                    try {
                        field.setDataType(field.getDataType());
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
            tablePropertiesSql += " COMMENT='" + tapTable.getComment() + "'";
        }
        return String.format(CREATE_TABLE_TEMPLATE, database, tapTable.getId(), fieldSql, tablePropertiesSql);
    }

    public static List<String> createTable(TapTable table, TidbConfig config) {
        List<String> sqls = new ArrayList<>();
        Map<String, TapField> fields = table.getNameFieldMap();

        String formatTableName = formatTableName(table, config);
        // create table only if table does not exist
        StringBuilder createTableBuilder = new StringBuilder()
                .append(String.format(TIDB_OBJECT_EXISTENCE_NULL_TEMPLATE, formatTableName))
                .append("create table ")
                .append(formatTableName)
                .append("(\n");

        Optional<TapField> autoincrementFieldOp = fields.values().stream().filter(TapField::getAutoInc).findFirst();
        // append field
        List<String> fieldLines = new ArrayList<>();
        for (TapField field : fields.values()) {
            StringBuilder fieldBuilder = new StringBuilder();
            fieldBuilder.append("  ").append(formatFieldName(field.getName()));
            fieldBuilder.append(" ").append(field.getDataType());
            // autoincrement, only one identity field is allowed in DDL
            if (field.getAutoInc() && autoincrementFieldOp.isPresent() && field.getName().equals(autoincrementFieldOp.get().getName())) {
                fieldBuilder.append(" identity");
            }

            // default value
            if (null != field.getDefaultValue()) {
                fieldBuilder.append(" default ");
                if (field.getDefaultValue() instanceof Number) {
                    fieldBuilder.append(field.getDefaultValue());
                } else {
                    fieldBuilder.append("'").append(field.getDefaultValue()).append("'");
                }
            }

            // nullable
            if (!field.getNullable()) {
                fieldBuilder.append(" not null");
            }

            fieldLines.add(fieldBuilder.toString());
        }
        // append primary key fields
        // TODO(dexter): do not specify pk of source connection is mssql
        Collection<String> primaryKeys = table.primaryKeys();
        if (!primaryKeys.isEmpty()) {
            StringBuilder pkBuilder = new StringBuilder();
            Optional<TapField> constraintOp = fields.values().stream().filter(field -> EmptyKit.isNotBlank(field.getConstraint())).findFirst();
            constraintOp.ifPresent(tapField -> pkBuilder.append("constraint ").append(tapField.getConstraint()).append(" "));
            pkBuilder.append("primary key (").append(String.join(",", primaryKeys)).append(")");
            fieldLines.add(pkBuilder.toString());
        }

        createTableBuilder.append(String.join(",\n", fieldLines)).append("\n)");
        sqls.add(createTableBuilder.toString());

        // table comment
        if (EmptyKit.isNotBlank(table.getComment())) {
            sqls.add(String.format(TIDB_TABLE_COMMENT_TEMPLATE,
                    table.getComment(), config.getSchema(), table.getName()));
        }
        // field comment
        fields.forEach((columnName, field) -> {
            if (EmptyKit.isNotBlank(field.getComment())) {
                sqls.add(String.format(TIDB_FIELD_COMMENT_TEMPLATE,
                        field.getComment(), config.getSchema(), table.getName(), field.getName()));
            }
        });

        return sqls;
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
                defaultValue = StringUtils.replace(defaultValue, "'", "\\'");
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
            comment = comment.replace("'", "''");
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

    private static final String TIDB_FIELD_TEMPLATE = "`%s`";
    private static final String TIDB_TABLE_TEMPLATE = "%s.%s";//"[%s].[%s].[%s]";
    private static final String TIDB_TABLE_WITHOUT_DATABASE_TEMPLATE = "%s.%s";




    public static String formatTableName(String database, String schema, String table) {
        return String.format(TIDB_TABLE_TEMPLATE, escape(database), escape(schema));//, escape(table)
    }

    public static String formatTableName(TapTable table, TidbConfig config) {
        return formatTableName(config.getDatabase(), table.getId(), table.getName());
    }

    public static String formatTableName(String schema, String table) {
        return String.format(TIDB_TABLE_WITHOUT_DATABASE_TEMPLATE, escape(schema), escape(table));
    }

    public static String formatFieldName(String fieldName) {
        return String.format(TIDB_FIELD_TEMPLATE, fieldName);
    }

    public static String escape(String name) {
        // these conversions escape the special characters in database(eg. xxx-{()}][\|!@#$%^&*-_=+/>.<,)
        return Objects.nonNull(name) ? name.replaceAll("\\]", "]]") : null;
    }

    public static String buildSqlByAdvanceFilter(TapTable table, TidbConfig config, TapAdvanceFilter filter) {
        StringBuilder builder = new StringBuilder("SELECT ");
        if (null != filter.getLimit()) {
            builder.append("TOP ").append(filter.getLimit()).append(" ");
        }
        Projection projection = filter.getProjection();
        if (EmptyKit.isNull(projection) || (EmptyKit.isEmpty(projection.getIncludeFields()) && EmptyKit.isEmpty(projection.getExcludeFields()))) {
            builder.append("*");
        } else {
            builder.append("`");
            if (EmptyKit.isNotEmpty(filter.getProjection().getIncludeFields())) {
                builder.append(String.join("`,`", filter.getProjection().getIncludeFields()));
            } else {
                builder.append(table.getNameFieldMap().keySet().stream()
                        .filter(tapField -> !filter.getProjection().getExcludeFields().contains(tapField)).collect(Collectors.joining("`,`")));
            }
            builder.append("`");
        }
        builder.append(" FROM ").append(TidbSqlMaker.formatTableName(table, config));

        if (EmptyKit.isNotEmpty(filter.getMatch()) || EmptyKit.isNotEmpty(filter.getOperators())) {
            builder.append(" WHERE ");
            builder.append(new CommonSqlMaker().buildKeyAndValue(filter.getMatch(), "AND", "="));
        }
        if (EmptyKit.isNotEmpty(filter.getOperators())) {
            if (EmptyKit.isNotEmpty(filter.getMatch())) {
                builder.append("AND ");
            }
            builder.append(filter.getOperators().stream().map(v -> v.toString("`")).collect(Collectors.joining(" AND "))).append(' ');
        }
        if (EmptyKit.isNotEmpty(filter.getSortOnList())) {
            builder.append("ORDER BY ");
            builder.append(filter.getSortOnList().stream().map(v -> v.toString("`")).collect(Collectors.joining(", "))).append(' ');
        }

//        Integer skip = filter.getSkip();
//        if (null != skip) {
//            builder.append("OFFSET ").append(filter.getSkip()).append(" ROWS ");
//        }

        return builder.toString();
    }

    public static String getOrderByUniqueKey(TapTable tapTable) {
        StringBuilder orderBy = new StringBuilder();
        orderBy.append(" ORDER BY ");
        List<TapIndex> indexList = tapTable.getIndexList();
        //has no indexes, need each field
        if (EmptyKit.isEmpty(indexList)) {
            orderBy.append(tapTable.getNameFieldMap().keySet().stream().map(field -> "`" + field + "`")
                    .reduce((v1, v2) -> v1 + ", " + v2).orElseGet(String::new));
        }
        //has indexes but no unique
        else if (indexList.stream().noneMatch(TapIndex::isUnique)) {
            TapIndex index = indexList.stream().findFirst().orElseGet(TapIndex::new);
            orderBy.append(index.getIndexFields().stream().map(field -> "`" + field.getName() + "` " + (field.getFieldAsc() ? "ASC" : "DESC"))
                    .reduce((v1, v2) -> v1 + ", " + v2).orElseGet(String::new));
            List<String> indexFields = index.getIndexFields().stream().map(TapIndexField::getName).collect(Collectors.toList());
            if (tapTable.getNameFieldMap().size() > indexFields.size()) {
                orderBy.append(',');
                orderBy.append(tapTable.getNameFieldMap().keySet().stream().filter(key -> !indexFields.contains(key)).map(field -> "`" + field + "`")
                        .reduce((v1, v2) -> v1 + ", " + v2).orElseGet(String::new));
            }
        }
        //has unique indexes
        else {
            TapIndex uniqueIndex = indexList.stream().filter(TapIndex::isUnique).findFirst().orElseGet(TapIndex::new);
            orderBy.append(uniqueIndex.getIndexFields().stream().map(field -> "`" + field.getName() + "` " + (field.getFieldAsc() ? "ASC" : "DESC"))
                    .reduce((v1, v2) -> v1 + ", " + v2).orElseGet(String::new));
        }
        return orderBy.toString();
    }
}
