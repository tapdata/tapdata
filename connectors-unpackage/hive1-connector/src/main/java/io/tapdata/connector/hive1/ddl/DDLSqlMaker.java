package io.tapdata.connector.hive1.ddl;

import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.pdk.apis.context.TapConnectorContext;

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


//	default void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
//		throw new UnsupportedOperationException();
//	}
}
