package io.tapdata.connector.postgres.ddl.sqlmaker;

import io.tapdata.connector.postgres.ddl.DDLSqlMaker;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
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
			StringBuilder addFieldSql = new StringBuilder(String.format(ALTER_TABLE_PREFIX, database, schema, tableId)).append(" add");
			String fieldName = newField.getName();
			if (StringUtils.isNotBlank(fieldName)) {
				addFieldSql.append(" `").append(fieldName).append("`");
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
			sqls.add(addFieldSql.toString());

			String comment = newField.getComment();
			if (StringUtils.isNotBlank(comment)) {
				sqls.add("comment on column " + String.format(TABLE_NAME_FORMAT, database, schema, tableId) + "." + String.format(COLUMN_NAME_FORMAT, fieldName) + " is '" + comment + "'");
			}
		}
		return sqls;
	}

	@Override
	public List<String> alterColumnAttr(TapConnectorContext tapConnectorContext, TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent) {
		return DDLSqlMaker.super.alterColumnAttr(tapConnectorContext, tapAlterFieldAttributesEvent);
	}

	@Override
	public List<String> alterColumnName(TapConnectorContext tapConnectorContext, TapAlterFieldNameEvent tapAlterFieldNameEvent) {
		return DDLSqlMaker.super.alterColumnName(tapConnectorContext, tapAlterFieldNameEvent);
	}

	@Override
	public List<String> dropColumn(TapConnectorContext tapConnectorContext, TapDropFieldEvent tapDropFieldEvent) {
		return DDLSqlMaker.super.dropColumn(tapConnectorContext, tapDropFieldEvent);
	}
}
