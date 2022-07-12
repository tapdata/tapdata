package io.tapdata.connector.mysql.ddl.sqlmaker;

import io.tapdata.connector.mysql.ddl.DDLSqlMaker;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-07-11 17:44
 **/
public class MysqlDDLSqlMaker implements DDLSqlMaker {
	private final static String ALTER_TABLE_PREFIX = "alter table `%s`.`%s`";

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
			String comment = newField.getComment();
			if (StringUtils.isNotBlank(comment)) {
				sql.append(" '").append(comment).append("'");
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
			sql.append(" ").append(dataTypeChange.getAfter());
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
			sql.append(" '").append(commentChange.getAfter()).append("'");
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
		return Collections.singletonList(String.format(ALTER_TABLE_PREFIX, database, tableId) + " rename `" + before + "` to `" + after + "`");
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
