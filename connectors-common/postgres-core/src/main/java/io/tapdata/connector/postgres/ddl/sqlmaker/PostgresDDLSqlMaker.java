package io.tapdata.connector.postgres.ddl.sqlmaker;

import io.tapdata.connector.postgres.ddl.DDLSqlMaker;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-07-12 10:56
 **/
public class PostgresDDLSqlMaker implements DDLSqlMaker {
	private final static String TABLE_NAME_FORMAT = "\"%s\".\"%s\".\"%s\"";
	private final static String ALTER_TABLE_PREFIX = "alter table " + TABLE_NAME_FORMAT;
	private final static String COLUMN_NAME_FORMAT = "\"%s\"";

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
		String schema = tapConnectorContext.getConnectionConfig().getString("schema");
		String tableId = tapNewFieldEvent.getTableId();
		if (StringUtils.isBlank(tableId)) {
			throw new RuntimeException("Append add column ddl sql failed, table name is blank");
		}
		for (TapField newField : newFields) {
			StringBuilder addFieldSql = new StringBuilder(String.format(ALTER_TABLE_PREFIX, database, schema, tableId)).append(" add column");
			String fieldName = newField.getName();
			if (StringUtils.isNotBlank(fieldName)) {
				addFieldSql.append(" ").append(String.format(COLUMN_NAME_FORMAT, fieldName));
			} else {
				throw new RuntimeException("Append add column ddl sql failed, field name is blank");
			}
			String dataType = newField.getDataType();
			if (StringUtils.isNotBlank(dataType)) {
				addFieldSql.append(" ").append(dataType);
			} else {
				throw new RuntimeException("Append add column ddl sql failed, data type is blank");
			}
			Boolean nullable = newField.getNullable();
			if (null != nullable) {
				if (nullable) {
					addFieldSql.append(" null");
				} else {
					addFieldSql.append(" not null");
				}
			}
			Object defaultValue = newField.getDefaultValue();
			if (null != defaultValue) {
				addFieldSql.append(" default '").append(defaultValue).append("'");
			}
			sqls.add(addFieldSql.toString());

			String comment = newField.getComment();
			if (StringUtils.isNotBlank(comment)) {
				sqls.add("comment on column " + String.format(TABLE_NAME_FORMAT, database, schema, tableId) + "." + String.format(COLUMN_NAME_FORMAT, fieldName) + " is '" + comment + "'");
			}

			Boolean primaryKey = newField.getPrimaryKey();
			if (null != primaryKey && primaryKey) {
				TapLogger.warn(PostgresDDLSqlMaker.class.getSimpleName(), "Alter postgresql table's primary key does not supported, please do it manually");
			}
		}
		return sqls;
	}

	@Override
	public List<String> alterColumnAttr(TapConnectorContext tapConnectorContext, TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent) {
		List<String> sqls = new ArrayList<>();
		if (null == tapAlterFieldAttributesEvent) {
			return null;
		}
		String database = tapConnectorContext.getConnectionConfig().getString("database");
		String schema = tapConnectorContext.getConnectionConfig().getString("schema");
		String tableId = tapAlterFieldAttributesEvent.getTableId();
		if (StringUtils.isBlank(tableId)) {
			throw new RuntimeException("Append alter column attr ddl sql failed, table name is blank");
		}
		String fieldName = tapAlterFieldAttributesEvent.getFieldName();
		ValueChange<String> dataTypeChange = tapAlterFieldAttributesEvent.getDataTypeChange();
		if (null != dataTypeChange && StringUtils.isNotBlank(dataTypeChange.getAfter())) {
			sqls.add(String.format(ALTER_TABLE_PREFIX, database, schema, tableId) + " alter column " + String.format(COLUMN_NAME_FORMAT, fieldName) + " set data type " + dataTypeChange.getAfter());
		}
		ValueChange<Boolean> nullableChange = tapAlterFieldAttributesEvent.getNullableChange();
		if (null != nullableChange && null != nullableChange.getAfter()) {
			sqls.add(String.format(String.format(ALTER_TABLE_PREFIX, database, schema, tableId) + " alter column " + COLUMN_NAME_FORMAT, fieldName) + (nullableChange.getAfter() ? " drop " : " set ") + " not null");
		}
		ValueChange<String> commentChange = tapAlterFieldAttributesEvent.getCommentChange();
		if (null != commentChange && StringUtils.isNotBlank(commentChange.getAfter())) {
			sqls.add("comment on column " + String.format(TABLE_NAME_FORMAT, database, schema, tableId) + "." + String.format(COLUMN_NAME_FORMAT, fieldName) + " is '" + commentChange.getAfter() + "'");
		}
		ValueChange<Integer> primaryChange = tapAlterFieldAttributesEvent.getPrimaryChange();
		if (null != primaryChange && null != primaryChange.getAfter() && primaryChange.getAfter() > 0) {
			TapLogger.warn(PostgresDDLSqlMaker.class.getSimpleName(), "Alter postgresql table's primary key does not supported, please do it manually");
		}
		return sqls;
	}

	@Override
	public List<String> alterColumnName(TapConnectorContext tapConnectorContext, TapAlterFieldNameEvent tapAlterFieldNameEvent) {
		if (null == tapAlterFieldNameEvent) {
			return null;
		}
		String database = tapConnectorContext.getConnectionConfig().getString("database");
		String schema = tapConnectorContext.getConnectionConfig().getString("schema");
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
		return Collections.singletonList(String.format(ALTER_TABLE_PREFIX, database, schema, tableId) + " rename column " + String.format(COLUMN_NAME_FORMAT, before) + " to " + String.format(COLUMN_NAME_FORMAT, after));
	}

	@Override
	public List<String> dropColumn(TapConnectorContext tapConnectorContext, TapDropFieldEvent tapDropFieldEvent) {
		if (null == tapDropFieldEvent) {
			return null;
		}
		String database = tapConnectorContext.getConnectionConfig().getString("database");
		String schema = tapConnectorContext.getConnectionConfig().getString("schema");
		String tableId = tapDropFieldEvent.getTableId();
		if (StringUtils.isBlank(tableId)) {
			throw new RuntimeException("Append drop column ddl sql failed, table name is blank");
		}
		String fieldName = tapDropFieldEvent.getFieldName();
		if (StringUtils.isBlank(fieldName)) {
			throw new RuntimeException("Append drop column ddl sql failed, field name is blank");
		}
		return Collections.singletonList(String.format(ALTER_TABLE_PREFIX, database, schema, tableId) + " drop column " + String.format(COLUMN_NAME_FORMAT, fieldName));
	}
}
