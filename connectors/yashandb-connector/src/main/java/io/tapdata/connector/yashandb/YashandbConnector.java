package io.tapdata.connector.yashandb;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.yashandb.config.YashandbConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Author:Skeet
 * Date: 2023/5/16
 **/
public class YashandbConnector extends ConnectorBase {
    protected YashandbConfig yashandbConfig;
    private YashandbJdbcContext yashandbJdbcContext;
    private YashandbTest yashandbTest;
    private YashandbContext yashandbContext;
    private String YashandbVersion;
    private YashandbDDLInstance yashandbDDLInstance;

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        yashandbConfig = (YashandbConfig) new YashandbConfig().load(connectionContext.getConnectionConfig());
        yashandbTest = new YashandbTest(yashandbConfig, testItem -> {
        }).initContext();
        yashandbJdbcContext = new YashandbJdbcContext(yashandbConfig);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        EmptyKit.closeQuietly(yashandbJdbcContext);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry tapCodecsRegistry) {
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);

    }

    private CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        String database = yashandbContext.getYashandbConfig().getDatabase();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        List<DataMap> tableNames = yashandbJdbcContext.queryAllTables(Collections.singletonList(tapTable.getId()));
        if (tableNames.contains(tapTable.getId())) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }
        Collection<String> primaryKeys = tapTable.primaryKeys(true);

        return createTableOptions;
    }

    @Override
    public void discoverSchema(TapConnectionContext tapConnectionContext, List<String> list, int i, Consumer<List<TapTable>> consumer) throws Throwable {

    }

    private void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
        yashandbJdbcContext.queryAllTables(TapSimplify.list(), batchSize, listConsumer);
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            yashandbJdbcContext.execute("DROP TABLE IF EXISTS " + yashandbConfig.getDatabase() + tapDropTableEvent.getTableId());
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        try {
            if (yashandbJdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() == 1) {
                yashandbJdbcContext.execute("TRUNCATE TABLE " + yashandbConfig.getDatabase() + tapClearTableEvent.getTableId());
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> events, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {

    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext tapConnectionContext, Consumer<TestItem> consumer) throws Throwable {
        return null;
    }

    @Override
    public int tableCount(TapConnectionContext tapConnectionContext) throws Throwable {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        AtomicInteger count = new AtomicInteger(0);
        this.yashandbJdbcContext.query(String.format("SELECT COUNT(*) AS table_count FROM ALL_TABLES WHERE OWNER = '%s'", database), rs -> {
            if (rs.next()) {
                count.set(Integer.parseInt(rs.getString("count")));
            }
        });
        return count.get();
    }
}
