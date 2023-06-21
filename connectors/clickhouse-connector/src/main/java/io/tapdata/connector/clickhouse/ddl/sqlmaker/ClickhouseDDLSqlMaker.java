package io.tapdata.connector.clickhouse.ddl.sqlmaker;

import io.tapdata.common.ddl.DDLSqlMaker;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

public class ClickhouseDDLSqlMaker implements DDLSqlMaker {

    private final static String TABLE_NAME_FORMAT = "\"%s\".\"%s\"";
    private final static String ALTER_TABLE_PREFIX = "alter table " + TABLE_NAME_FORMAT;

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
            throw new RuntimeException("CK Append add column ddl sql failed, table name is blank");
        }
        for (TapField newField : newFields) {
            newField.setDataType(newField.getDataType().replace("unsigned","").replace("UNSIGNED",""));
            StringBuilder sql = new StringBuilder(String.format(ALTER_TABLE_PREFIX, database, tableId)).append(" add column ");
            String fieldName = newField.getName();
            if (StringUtils.isNotBlank(fieldName)) {
                sql.append(" `").append(fieldName).append("`");
            } else {
                throw new RuntimeException("CK Append add column ddl sql failed, field name is blank");
            }
            String dataType = newField.getDataType();
            if (StringUtils.isNotBlank(dataType)) {
                if (newField.getNullable() != null && newField.getNullable()) {
                    sql.append("Nullable(").append(newField.getDataType()).append(")").append(' ');
                } else {
                    sql.append(" ").append(dataType);
                }
            } else {
                throw new RuntimeException("CK Append add column ddl sql failed, data type is blank");
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
            throw new RuntimeException("CK Append alter column attr ddl sql failed, table name is blank");
        }
        StringBuilder sql = new StringBuilder(String.format(ALTER_TABLE_PREFIX, database, tableId)).append(" modify column");
        String fieldName = tapAlterFieldAttributesEvent.getFieldName();
        if (StringUtils.isNotBlank(fieldName)) {
            sql.append(" `").append(fieldName).append("`");
        } else {
            throw new RuntimeException("CK Append alter column attr ddl sql failed, field name is blank");
        }
        ValueChange<String> dataTypeChange = tapAlterFieldAttributesEvent.getDataTypeChange();
        dataTypeChange.setAfter(dataTypeChange.getAfter().replace("unsigned","").replace("UNSIGNED",""));
        if (StringUtils.isNotBlank(dataTypeChange.getAfter())) {
            sql.append(" ").append(dataTypeChange.getAfter());
        } else {
            throw new RuntimeException("CK Append alter column attr ddl sql failed, data type is blank");
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
            throw new RuntimeException("CK Append alter column name ddl sql failed, table name is blank");
        }
        ValueChange<String> nameChange = tapAlterFieldNameEvent.getNameChange();
        if (null == nameChange) {
            throw new RuntimeException("CK Append alter column name ddl sql failed, change name object is null");
        }
        String before = nameChange.getBefore();
        String after = nameChange.getAfter();
        if (StringUtils.isBlank(before)) {
            throw new RuntimeException("CK Append alter column name ddl sql failed, old column name is blank");
        }
        if (StringUtils.isBlank(after)) {
            throw new RuntimeException("CK Append alter column name ddl sql failed, new column name is blank");
        }
        return Collections.singletonList(String.format(ALTER_TABLE_PREFIX, database, tableId) + " rename column `" + before + "` to `" + after + "`");
    }

    @Override
    public List<String> dropColumn(TapConnectorContext tapConnectorContext, TapDropFieldEvent tapDropFieldEvent) {
        if (null == tapDropFieldEvent) {
            return null;
        }
        String database = tapConnectorContext.getConnectionConfig().getString("database");
        String tableId = tapDropFieldEvent.getTableId();
        if (StringUtils.isBlank(tableId)) {
            throw new RuntimeException("CK Append drop column ddl sql failed, table name is blank");
        }
        String fieldName = tapDropFieldEvent.getFieldName();
        if (StringUtils.isBlank(fieldName)) {
            throw new RuntimeException("CK Append drop column ddl sql failed, field name is blank");
        }
        return Collections.singletonList(String.format(ALTER_TABLE_PREFIX, database, tableId) + " drop column`" + fieldName + "`");
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
    public static String buildColumnDefinition(TapTable tapTable, boolean needComment) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        //no primary key,need judge logic primary key
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
//        Collection<String> logicPrimaryKeys = EmptyKit.isNotEmpty(primaryKeys) ? Collections.emptyList() : tapTable.primaryKeys(true);
        return nameFieldMap.entrySet().stream().sorted(Comparator.comparing(v ->
                EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).map(v -> { //pos may be null
            StringBuilder builder = new StringBuilder();
            TapField tapField = v.getValue();
            //ignore those which has no dataType
            if (tapField.getDataType() == null) {
                return "";
            }
            tapField.setDataType(tapField.getDataType().replace("unsigned","").replace("UNSIGNED",""));
            builder.append('\"').append(tapField.getName()).append("\" ");
            //null to omit
            if (tapField.getNullable() != null && tapField.getNullable() && !primaryKeys.contains(tapField.getName())) {
                builder.append("Nullable(").append(tapField.getDataType()).append(")").append(' ');
            }else{
                builder.append(tapField.getDataType()).append(' ');
            }


            //null to omit
            if (tapField.getDefaultValue() != null && !"".equals(tapField.getDefaultValue())) {
                builder.append("DEFAULT").append(' ');
                if (tapField.getDefaultValue() instanceof Number) {
                    builder.append(tapField.getDefaultValue()).append(' ');
                } else {
                    builder.append("'").append(tapField.getDefaultValue()).append("' ");
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
}
