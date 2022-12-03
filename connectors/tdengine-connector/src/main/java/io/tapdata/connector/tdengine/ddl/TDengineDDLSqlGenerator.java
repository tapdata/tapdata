package io.tapdata.connector.tdengine.ddl;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.ddl.DDLSqlGenerator;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.kit.EmptyKit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TDengineDDLSqlGenerator implements DDLSqlGenerator {
    private final static String TABLE_NAME_FORMAT = "%s.%s";
    private final static String ALTER_TABLE_PREFIX = "alter table " + TABLE_NAME_FORMAT;
    private final static String COLUMN_NAME_FORMAT = "%s";

    @Override
    public List<String> addColumn(CommonDbConfig config, TapNewFieldEvent tapNewFieldEvent) {
        List<String> sqls = new ArrayList<>();
        if (null == tapNewFieldEvent) {
            return null;
        }
        List<TapField> newFields = tapNewFieldEvent.getNewFields();
        if (null == newFields) {
            return null;
        }
        String database = config.getDatabase();
        String tableId = tapNewFieldEvent.getTableId();
        if (EmptyKit.isBlank(tableId)) {
            throw new RuntimeException("Append add column ddl sql failed, table name is blank");
        }
        for (TapField newField : newFields) {
            StringBuilder addFieldSql = new StringBuilder(String.format(ALTER_TABLE_PREFIX, database, tableId)).append(" ADD COLUMN");
            String fieldName = newField.getName();
            if (EmptyKit.isNotBlank(fieldName)) {
                addFieldSql.append(" ").append(String.format(COLUMN_NAME_FORMAT, fieldName));
            } else {
                throw new RuntimeException("Append add column ddl sql failed, field name is blank");
            }
            String dataType = newField.getDataType();
            if (EmptyKit.isNotBlank(dataType)) {
                addFieldSql.append(" ").append(dataType);
            } else {
                throw new RuntimeException("Append add column ddl sql failed, data type is blank");
            }
            sqls.add(addFieldSql.toString());
        }
        return sqls;
    }

    @Override
    public List<String> alterColumnAttr(CommonDbConfig config, TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent) {
        List<String> sqls = new ArrayList<>();
        if (null == tapAlterFieldAttributesEvent) {
            return null;
        }
        String database = config.getDatabase();
        String tableId = tapAlterFieldAttributesEvent.getTableId();
        if (EmptyKit.isBlank(tableId)) {
            throw new RuntimeException("Append alter column attr ddl sql failed, table name is blank");
        }
        String fieldName = tapAlterFieldAttributesEvent.getFieldName();
        ValueChange<String> dataTypeChange = tapAlterFieldAttributesEvent.getDataTypeChange();
        if (null != dataTypeChange && EmptyKit.isNotBlank(dataTypeChange.getAfter())) {
            sqls.add(String.format(ALTER_TABLE_PREFIX, database, tableId) + " MODIFY COLUMN " + String.format(COLUMN_NAME_FORMAT, fieldName) + " " + dataTypeChange.getAfter());
        }
        ValueChange<Integer> primaryChange = tapAlterFieldAttributesEvent.getPrimaryChange();
        if (null != primaryChange && null != primaryChange.getAfter() && primaryChange.getAfter() > 0) {
            TapLogger.warn(TDengineDDLSqlGenerator.class.getSimpleName(), "Alter postgresql table's primary key does not supported, please do it manually");
        }
        return sqls;
    }

    @Override
    public List<String> alterColumnName(CommonDbConfig config, TapAlterFieldNameEvent tapAlterFieldNameEvent) {
        if (null == tapAlterFieldNameEvent) {
            return null;
        }
        String database = config.getDatabase();
        String tableId = tapAlterFieldNameEvent.getTableId();
        if (EmptyKit.isBlank(tableId)) {
            throw new RuntimeException("Append alter column name ddl sql failed, table name is blank");
        }
        ValueChange<String> nameChange = tapAlterFieldNameEvent.getNameChange();
        if (null == nameChange) {
            throw new RuntimeException("Append alter column name ddl sql failed, change name object is null");
        }
        String before = nameChange.getBefore();
        String after = nameChange.getAfter();
        if (EmptyKit.isBlank(before)) {
            throw new RuntimeException("Append alter column name ddl sql failed, old column name is blank");
        }
        if (EmptyKit.isBlank(after)) {
            throw new RuntimeException("Append alter column name ddl sql failed, new column name is blank");
        }
        return Collections.singletonList(String.format(ALTER_TABLE_PREFIX, database, tableId) + " RENAME COLUMN " + String.format(COLUMN_NAME_FORMAT, before) + " " + String.format(COLUMN_NAME_FORMAT, after));
    }

    @Override
    public List<String> dropColumn(CommonDbConfig config, TapDropFieldEvent tapDropFieldEvent) {
        if (null == tapDropFieldEvent) {
            return null;
        }
        String database = config.getDatabase();
        String schema = config.getSchema();
        String tableId = tapDropFieldEvent.getTableId();
        if (EmptyKit.isBlank(tableId)) {
            throw new RuntimeException("Append drop column ddl sql failed, table name is blank");
        }
        String fieldName = tapDropFieldEvent.getFieldName();
        if (EmptyKit.isBlank(fieldName)) {
            throw new RuntimeException("Append drop column ddl sql failed, field name is blank");
        }
        return Collections.singletonList(String.format(ALTER_TABLE_PREFIX, database, tableId) + " drop column " + String.format(COLUMN_NAME_FORMAT, fieldName));
    }
}
