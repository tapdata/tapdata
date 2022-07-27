package io.tapdata.connector.clickhouse.ddl;

import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-07-11 17:25
 **/
public interface DDLSqlMaker {
	default List<String> addColumn(TapConnectorContext tapConnectorContext, TapNewFieldEvent tapNewFieldEvent){
		throw new UnsupportedOperationException();
	}

	default List<String> alterColumnAttr(TapConnectorContext tapConnectorContext, TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent){
		throw new UnsupportedOperationException();
	}

	default List<String> alterColumnName(TapConnectorContext tapConnectorContext, TapAlterFieldNameEvent tapAlterFieldNameEvent){
		throw new UnsupportedOperationException();
	}

	default List<String> dropColumn(TapConnectorContext tapConnectorContext, TapDropFieldEvent tapDropFieldEvent){
		throw new UnsupportedOperationException();
	}

}
