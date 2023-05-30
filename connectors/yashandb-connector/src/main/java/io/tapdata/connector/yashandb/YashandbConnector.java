package io.tapdata.connector.yashandb;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.CommonDbConnector;
import io.tapdata.connector.yashandb.config.YashandbConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Author:Skeet
 * Date: 2023/5/16
 **/
@TapConnectorClass("yashandb-spec.json")
public class YashandbConnector extends CommonDbConnector {
    protected YashandbConfig yashandbConfig;
    private YashandbJdbcContext yashandbJdbcContext;
    private YashandbTest yashandbTest;
    private YashandbContext yashandbContext;

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        yashandbConfig = (YashandbConfig) new YashandbConfig().load(connectionContext.getConnectionConfig());
        yashandbTest = new YashandbTest(yashandbConfig, testItem -> {
        }).initContext();
        yashandbJdbcContext = new YashandbJdbcContext(yashandbConfig);
        commonSqlMaker = new YashandbSqlMaker().closeNotNull(yashandbConfig.getCloseNotNull());
        jdbcContext = yashandbJdbcContext;
        commonDbConfig = yashandbConfig;
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        EmptyKit.closeQuietly(yashandbJdbcContext);
        EmptyKit.closeQuietly(yashandbTest);
        EmptyKit.closeQuietly(jdbcContext);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);


        codecRegistry.registerFromTapValue(TapBooleanValue.class, "INTEGER", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return tapValue.getValue() ? 1 : 0;
            return 0;
        });

        codecRegistry.registerFromTapValue(TapArrayValue.class, "CLOB", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });

        codecRegistry.registerFromTapValue(TapMapValue.class, "CLOB", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });

        codecRegistry.registerFromTapValue(TapBinaryValue.class, "CLOB", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });

        codecRegistry.registerFromTapValue(TapRawValue.class, "CLOB", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return tapRawValue.getValue().toString();
            return "null";
        });

        codecRegistry.registerFromTapValue(TapDateTimeValue.class, "TIMESTAMP(9)", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return formatTapDateTime(tapValue.getValue(), "YYYY-MM-DD HH:MM:SS.ssssss");
            }
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapYearValue.class, "CHAR(4)", tapYearValue -> formatTapDateTime(tapYearValue.getValue(), "yyyy"));
        codecRegistry.registerFromTapValue(TapDateValue.class, "DATE", tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));


    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws SQLException {
        List<DataMap> tableList = jdbcContext.queryAllTables(tables);
        List<String> list = new ArrayList<>();
        if (tables == null || tables.isEmpty()) {
            accept(tables.stream().map(ConnectorBase::table).collect(Collectors.toList()), tableSize, consumer);
            return;
        }

        List<TapTable> tapTableList = TapSimplify.list();
        if (tables.size() > 0) {
            for (String s : tables) {
                if (!list.contains(s) && tables.contains(s))
                    list.add(s);
            }
            for (String k : list) {
                tapTableList.add(table(k));
            }
        }
        accept(tapTableList, tableSize, consumer);
    }

    public void accept(List<TapTable> tables, int size, Consumer<List<TapTable>> consumer) {
        if (size > 0) {
            List<TapTable> group = new ArrayList<>();
            for (int index = 0; index < tables.size(); index++) {
                if (group.size() % size == 0 && !group.isEmpty()) {
                    consumer.accept(tables);
                    group = new ArrayList<>();
                }
                group.add(tables.get(index));
            }
            if (!group.isEmpty()) {
                consumer.accept(tables);
            }
            return;
        }
        consumer.accept(tables);
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> events, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        new YashandbRecordWriter(yashandbJdbcContext, table).write(events, writeListResultConsumer);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext tapConnectionContext, Consumer<TestItem> consumer) throws Throwable {
        yashandbConfig = (YashandbConfig) new YashandbConfig().load(tapConnectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(yashandbConfig.getConnectionString());
        try (
                YashandbTest yashandbTest = new YashandbTest(yashandbConfig, consumer).initContext()
        ) {
            yashandbTest.testOneByOne();
            return connectionOptions;
        }
    }
}
