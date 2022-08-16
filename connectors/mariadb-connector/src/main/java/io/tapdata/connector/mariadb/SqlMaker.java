package io.tapdata.connector.mariadb;

import io.tapdata.connector.mariadb.entity.MariadbSnapshotOffset;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;


public interface SqlMaker {
	String[] createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent, String version) throws Throwable;

	String selectSql(TapConnectorContext tapConnectorContext, TapTable tapTable, MariadbSnapshotOffset mariadbSnapshotOffset) throws Throwable;

	String selectSql(TapConnectorContext tapConnectorContext, TapTable tapTable, TapAdvanceFilter tapAdvanceFilter) throws Throwable;

	String createIndex(TapConnectorContext tapConnectorContext, TapTable tapTable, TapIndex tapIndex) throws Throwable;
}
