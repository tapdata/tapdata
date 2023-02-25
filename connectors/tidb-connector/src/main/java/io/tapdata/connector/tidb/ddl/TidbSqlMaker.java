package io.tapdata.connector.tidb.ddl;

import io.tapdata.common.ddl.DDLSqlMaker;
import io.tapdata.connector.mysql.MysqlMaker;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
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

    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE `%s`.`%s`(\n%s) %s";

    private final static String ALTER_TABLE_PREFIX = "alter table `%s`.`%s`";

    protected static final int DEFAULT_CONSTRAINT_NAME_MAX_LENGTH = 30;


    public String createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent){
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
            if(defaultValue.contains("'")){
                defaultValue = StringUtils.replace(defaultValue, "'", "\\'");
            }
            if(tapField.getTapType() instanceof TapNumber){
                defaultValue = defaultValue.trim();
            }
            fieldSql += " DEFAULT '" + defaultValue + "'";

        }

        // comment
        String comment = tapField.getComment();
        if (StringUtils.isNotBlank(comment)) {
            // try to escape the single quote in comments
            comment = comment.replace("'", "\\'");
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

    @Override
    public List<String> addColumn(TapConnectorContext tapConnectorContext, TapNewFieldEvent tapNewFieldEvent) {
        List<String> sqls = new ArrayList<>();
        if (null == tapNewFieldEvent) {
            return null;
        }
        List<TapField> newFields = tapNewFieldEvent.getNewFields();
        if (null == newFields) {
            return null;
        }
        String database = tapConnectorContext.getConnectionConfig().getString("database");
        String tableId = tapNewFieldEvent.getTableId();
        if (StringUtils.isBlank(tableId)) {
            throw new RuntimeException("Append add column ddl sql failed, table name is blank");
        }
        for (TapField newField : newFields) {
            StringBuilder sql = new StringBuilder(String.format(ALTER_TABLE_PREFIX, database, tableId)).append(" add");
            String fieldName = newField.getName();
            if (StringUtils.isNotBlank(fieldName)) {
                sql.append(" `").append(fieldName).append("`");
            } else {
                throw new RuntimeException("Append add column ddl sql failed, field name is blank");
            }
            String dataType = newField.getDataType();
//            try {
//                dataType = MysqlUtil.fixDataType(dataType, version);
//            } catch (Exception e) {
//                TapLogger.warn(TAG, e.getMessage());
//            }
            if (StringUtils.isNotBlank(dataType)) {
                sql.append(" ").append(dataType);
            } else {
                throw new RuntimeException("Append add column ddl sql failed, data type is blank");
            }
            Boolean nullable = newField.getNullable();
            if (null != nullable) {
                if (nullable) {
                    sql.append(" null");
                } else {
                    sql.append(" not null");
                }
            }
            Object defaultValue = newField.getDefaultValue();
            if (null != defaultValue) {
                sql.append(" default '").append(defaultValue).append("'");
            }
            String comment = newField.getComment();
            if (StringUtils.isNotBlank(comment)) {
                sql.append(" comment '").append(comment).append("'");
            }
            Boolean primaryKey = newField.getPrimaryKey();
            if (null != primaryKey && primaryKey) {
                sql.append(" key");
            }
            sqls.add(sql.toString());
        }
        return sqls;
    }

    @Override
    public List<String> alterColumnAttr(TapConnectorContext tapConnectorContext, TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent) {
        if (null == tapAlterFieldAttributesEvent) {
            return null;
        }
        String database = tapConnectorContext.getConnectionConfig().getString("database");
        String tableId = tapAlterFieldAttributesEvent.getTableId();
        if (StringUtils.isBlank(tableId)) {
            throw new RuntimeException("Append alter column attr ddl sql failed, table name is blank");
        }
        StringBuilder sql = new StringBuilder(String.format(ALTER_TABLE_PREFIX, database, tableId)).append(" modify");
        String fieldName = tapAlterFieldAttributesEvent.getFieldName();
        if (StringUtils.isNotBlank(fieldName)) {
            sql.append(" `").append(fieldName).append("`");
        } else {
            throw new RuntimeException("Append alter column attr ddl sql failed, field name is blank");
        }
        ValueChange<String> dataTypeChange = tapAlterFieldAttributesEvent.getDataTypeChange();
        if (StringUtils.isNotBlank(dataTypeChange.getAfter())) {
            String dataTypeChangeAfter = dataTypeChange.getAfter();
            sql.append(" ").append(dataTypeChangeAfter);
        } else {
            throw new RuntimeException("Append alter column attr ddl sql failed, data type is blank");
        }
        ValueChange<Boolean> nullableChange = tapAlterFieldAttributesEvent.getNullableChange();
        if (null != nullableChange && null != nullableChange.getAfter()) {
            if (nullableChange.getAfter()) {
                sql.append(" null");
            } else {
                sql.append(" not null");
            }
        }
        ValueChange<String> commentChange = tapAlterFieldAttributesEvent.getCommentChange();
        if (null != commentChange && StringUtils.isNotBlank(commentChange.getAfter())) {
            sql.append(" comment '").append(commentChange.getAfter()).append("'");
        }
        ValueChange<Integer> primaryChange = tapAlterFieldAttributesEvent.getPrimaryChange();
        if (null != primaryChange && null != primaryChange.getAfter() && primaryChange.getAfter() > 0) {
            sql.append(" key");
        }
        return Collections.singletonList(sql.toString());
    }

    @Override
    public List<String> alterColumnName(TapConnectorContext tapConnectorContext, TapAlterFieldNameEvent tapAlterFieldNameEvent) {
        if (null == tapAlterFieldNameEvent) {
            return null;
        }
        String database = tapConnectorContext.getConnectionConfig().getString("database");
        String tableId = tapAlterFieldNameEvent.getTableId();
        if (StringUtils.isBlank(tableId)) {
            throw new RuntimeException("Append alter column name ddl sql failed, table name is blank");
        }
        ValueChange<String> nameChange = tapAlterFieldNameEvent.getNameChange();
        if (null == nameChange) {
            throw new RuntimeException("Append alter column name ddl sql failed, change name object is null");
        }
        String before = nameChange.getBefore();
        String after = nameChange.getAfter();
        if (StringUtils.isBlank(before)) {
            throw new RuntimeException("Append alter column name ddl sql failed, old column name is blank");
        }
        if (StringUtils.isBlank(after)) {
            throw new RuntimeException("Append alter column name ddl sql failed, new column name is blank");
        }
       // Integer subVersion = MysqlUtil.getSubVersion(version, 1);
        String sql = String.format(ALTER_TABLE_PREFIX, database, tableId);
       return Collections.singletonList(sql + " rename column `" + before + "` to `" + after + "`");

    }

    @Override
    public List<String> dropColumn(TapConnectorContext tapConnectorContext, TapDropFieldEvent tapDropFieldEvent) {
        if (null == tapDropFieldEvent) {
            return null;
        }
        String database = tapConnectorContext.getConnectionConfig().getString("database");
        String tableId = tapDropFieldEvent.getTableId();
        if (StringUtils.isBlank(tableId)) {
            throw new RuntimeException("Append drop column ddl sql failed, table name is blank");
        }
        String fieldName = tapDropFieldEvent.getFieldName();
        if (StringUtils.isBlank(fieldName)) {
            throw new RuntimeException("Append drop column ddl sql failed, field name is blank");
        }
        return Collections.singletonList(String.format(ALTER_TABLE_PREFIX, database, tableId) + " drop `" + fieldName + "`");
    }




}
