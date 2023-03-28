package io.tapdata.connector.selectdb;

import io.tapdata.common.ddl.DDLSqlMaker;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static io.tapdata.base.ConnectorBase.list;

/**
 * Author:Skeet
 * Date: 2023/1/13
 **/
public class SelectDbDDLSqlMaker implements DDLSqlMaker {
    public static final String TAG = SelectDbDDLSqlMaker.class.getSimpleName();
    protected final static String ALTER_TABLE_PREFIX = "ALTER TABLE `%s`.`%s`";

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
            StringBuilder sql = new StringBuilder(String.format(ALTER_TABLE_PREFIX, database, tableId)).append(" ADD COLUMN");
            String fieldName = newField.getName();
            if (StringUtils.isNotBlank(fieldName)) {
                sql.append(" `").append(fieldName).append("`");
            } else {
                throw new RuntimeException("Append add column ddl sql failed, field name is blank");
            }
            String dataType = newField.getDataType();
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
                sql.append(" DEFAULT '").append(defaultValue).append("'");
            }
            String comment = newField.getComment();
            if (StringUtils.isNotBlank(comment)) {
                sql.append(" comment '").append(comment).append("'");
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
        String before = tapConnectorContext.getTableMap().get(tableId).getNameFieldMap()
                .get(tapAlterFieldAttributesEvent.getFieldName()).getDataType();
        String after = tapAlterFieldAttributesEvent.getDataTypeChange().getAfter();
        if (after == null || after.equals(before)) {
            return list();
        }
        if (StringUtils.isBlank(tableId)) {
            throw new RuntimeException("Append alter column attr ddl sql failed, table name is blank");
        }
        StringBuilder sql = new StringBuilder(String.format(ALTER_TABLE_PREFIX, database, tableId)).append(" modify column");
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
                sql.append(" NULL");
            } else {
                sql.append(" NOT NULL");
            }
        }
        ValueChange<String> commentChange = tapAlterFieldAttributesEvent.getCommentChange();
        if (null != commentChange && StringUtils.isNotBlank(commentChange.getAfter())) {
            sql.append(" comment '").append(commentChange.getAfter()).append("'");
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
        String sql = String.format(ALTER_TABLE_PREFIX, database, tableId);
        TapTable tapTable = tapConnectorContext.getTableMap().get(tableId);
        if (tapTable == null) {
            throw new RuntimeException("Append alter column name ddl sql failed, tapTable is blank");
        }
        Optional<TapField> tapFieldOptional = tapTable.getNameFieldMap().entrySet().stream()
                .filter(e -> StringUtils.equals(e.getKey(), after)).map(Map.Entry::getValue).findFirst();
        if (!tapFieldOptional.isPresent()) {
            throw new RuntimeException("Append alter column name ddl sql failed, field is blank");
        }
        TapField field = tapFieldOptional.get();
        sql += " rename column `" + before + "` " + "`" + after + "` ";
        if (null != field.getAutoInc() && field.getAutoInc()) {
            if (field.getPrimaryKeyPos() == 1) {
                sql += " auto_increment";
            } else {
                TapLogger.warn(TAG, "Field \"{}\" cannot be auto increment in mysql, there can be only one auto column and it must be defined the first key", field.getName());
            }
        }
//        if (field.getNullable()) {
//            sql += " null";
//        } else {
//            sql += " not null";
//        }
        // default value
//        String defaultValue = field.getDefaultValue() == null ? "" : field.getDefaultValue().toString();
//        if (StringUtils.isNotBlank(defaultValue)) {
//            sql += " default '" + defaultValue + "'";
//        }

        // comment
//        String comment = field.getComment();
//        if (StringUtils.isNotBlank(comment)) {
//            // try to escape the single quote in comments
//            comment = comment.replace("'", "\\'");
//            sql += " comment '" + comment + "'";
//        }

//        Boolean primaryKey = field.getPrimaryKey();
//        if (null != primaryKey && primaryKey) {
//            sql += " key";
//        }
        return Collections.singletonList(sql);
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
        return Collections.singletonList(String.format(ALTER_TABLE_PREFIX, database, tableId) + " DROP COLUMN`" + fieldName + "`");
    }
}
