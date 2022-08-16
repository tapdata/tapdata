package io.tapdata.connector.mariadb.ddl.sqlmaker;

import io.tapdata.connector.mariadb.MariadbContext;
import io.tapdata.connector.mariadb.ddl.DDLSqlMaker;
import io.tapdata.connector.mariadb.util.MariadbUtil;
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
import java.util.concurrent.atomic.AtomicReference;


public class MariadbDDLSqlMaker implements DDLSqlMaker {
	private final static String ALTER_TABLE_PREFIX = "alter table `%s`.`%s`";
	public static final String TAG = MariadbDDLSqlMaker.class.getSimpleName();

	public final static String SELECT_COLUMN_TYPE =
			"select COLUMN_TYPE from information_schema.COLUMNS where TABLE_SCHEMA='%s' and table_name='%s' and COLUMN_NAME='%s'";
	private String version;

	public MariadbDDLSqlMaker(String version) {
		this.version = version;
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
			try {
				dataType = MariadbUtil.fixDataType(dataType, version);
			} catch (Exception e) {
				TapLogger.warn(TAG, e.getMessage());
			}
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
			try {
				dataTypeChangeAfter = MariadbUtil.fixDataType(dataTypeChangeAfter, version);
			} catch (Exception e) {
				TapLogger.warn(TAG, e.getMessage());
			}
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
	public List<String> alterColumnName(TapConnectorContext tapConnectorContext, TapAlterFieldNameEvent tapAlterFieldNameEvent,
										MariadbContext mariadbContext)  {
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
		AtomicReference<String> cloumnAttr = new AtomicReference<>("");
		try {
			mariadbContext.query(String.format(SELECT_COLUMN_TYPE, database, tableId,before), rs -> {
				if (rs.next()) {
					cloumnAttr.set(rs.getString("COLUMN_TYPE"));
				}
			});
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}

		return Collections.singletonList(String.format(ALTER_TABLE_PREFIX, database, tableId) + " change column " + before+ " " +after + " "+ cloumnAttr);
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
