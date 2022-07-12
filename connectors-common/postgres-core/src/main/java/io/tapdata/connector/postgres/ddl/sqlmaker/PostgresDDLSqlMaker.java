package io.tapdata.connector.postgres.ddl.sqlmaker;

import io.tapdata.connector.postgres.ddl.DDLSqlMaker;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-07-12 10:56
 **/
public class PostgresDDLSqlMaker implements DDLSqlMaker {
	@Override
	public List<String> addColumn(TapConnectorContext tapConnectorContext, TapNewFieldEvent tapNewFieldEvent) {
		return DDLSqlMaker.super.addColumn(tapConnectorContext, tapNewFieldEvent);
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
