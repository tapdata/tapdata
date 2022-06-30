package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.entity.MysqlSnapshotOffset;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;

/**
 * @author samuel
 * @Description
 * @create 2022-05-06 20:24
 **/
public interface SqlMaker {
	String[] createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent, String version) throws Throwable;

	String selectSql(TapConnectorContext tapConnectorContext, TapTable tapTable, MysqlSnapshotOffset mysqlSnapshotOffset) throws Throwable;

	String selectSql(TapConnectorContext tapConnectorContext, TapTable tapTable, TapAdvanceFilter tapAdvanceFilter) throws Throwable;

	String createIndex(TapConnectorContext tapConnectorContext, TapTable tapTable, TapIndex tapIndex) throws Throwable;
}
