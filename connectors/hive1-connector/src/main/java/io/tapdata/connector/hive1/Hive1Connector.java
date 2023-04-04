package io.tapdata.connector.hive1;

import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.hive1.config.Hive1Config;
import io.tapdata.connector.hive1.ddl.DDLSqlMaker;
import io.tapdata.connector.hive1.ddl.impl.Hive1JDBCSqlMaker;
import io.tapdata.connector.hive1.dml.Hive1Writer;
import io.tapdata.connector.hive1.dml.impl.Hive1WriterJDBC;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_hive1.json")
public class Hive1Connector extends ConnectorBase {


    public static final String TAG = Hive1Connector.class.getSimpleName();

    private Hive1Config hive1Config;

    private Hive1JdbcContext hive1JdbcContext;

    private static final int BATCH_ADVANCE_READ_LIMIT = 1000;

    private BiClassHandlers<TapFieldBaseEvent, TapConnectorContext, List<String>> fieldDDLHandlers;

    private String connectionTimezone;

    private DDLSqlMaker ddlSqlMaker;

    private Hive1Writer hive1Writer;

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        TapLogger.info("线程debug", "onStart当前线程为:{}", Thread.currentThread().getName());
        initConnection(connectionContext);
        ddlSqlMaker = new Hive1JDBCSqlMaker();

    }

    private void initConnection(TapConnectionContext connectionContext) throws Throwable {
        hive1Config = (Hive1Config) new Hive1Config().load(connectionContext.getConnectionConfig());
//        String hiveConnType = hive1Config.getHiveConnType();
//        if (StringUtils.isBlank(hiveConnType) || hiveConnType.equals("jdbc")) {
        hive1JdbcContext = new Hive1JdbcContext(hive1Config);
        this.connectionTimezone = connectionContext.getConnectionConfig().getString("timezone");
        if ("Database Timezone".equals(this.connectionTimezone) || StringUtils.isBlank(this.connectionTimezone)) {
            this.connectionTimezone = "08:00:00";
        }
        this.hive1Writer = new Hive1WriterJDBC(hive1JdbcContext, hive1Config);
//        }else{
//            this.hive1Writer = new Hive1WriterStream(hive1Config);
//        }
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        List<DataMap> tableList = hive1JdbcContext.queryAllTables(tables);
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        try {
            String database = connectionContext.getConnectionConfig().getString("database");
            tableLists.forEach(subList -> {
                List<TapTable> tapTableList = TapSimplify.list();
                List<DataMap> columnList1 = new ArrayList<>();
                subList.forEach(subTable -> {
                    //1.table name/comment
                    String table = subTable.getString("tab_name");
                    List<DataMap> columnList = hive1JdbcContext.queryColumnsOfTable(database, table);
                    columnList1.addAll(columnList);
                    TapTable tapTable = table(table);
//                    tapTable.setComment(subTable.getString("comment"));
                    List<String> primaryKey = TapSimplify.list();
                    AtomicInteger position = new AtomicInteger(1);
                    columnList.stream().forEach(col -> {
                        String columnName = col.getString("col_name");
                        String columnType = col.getString("data_type");
                        Boolean nullable = false;
                        TapField field = TapSimplify.field(columnName, columnType);
                        field.nullable(nullable);
//                                int ordinalPosition = Integer.parseInt(col.getString("position"));
                        field.pos(position.getAndIncrement());
                        tapTable.add(field);
                    });
                    tapTableList.add(tapTable);
                });
                if (CollectionUtils.isNotEmpty(columnList1)) {
                    consumer.accept(tapTableList);
                    tapTableList.clear();
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    @Override
    public void onStop(TapConnectionContext connectionContext) {
        TapLogger.info("线程debug", "onStop当前线程为:{}", Thread.currentThread().getName());
        EmptyKit.closeQuietly(hive1JdbcContext);
        Optional.ofNullable(this.hive1Writer).ifPresent(Hive1Writer::onDestroy);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {

        codecRegistry.registerFromTapValue(TapRawValue.class, "STRING", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "STRING", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "STRING", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapBinaryValue.class, "STRING", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null)
                return new String(Base64.encodeBase64(tapValue.getValue()));
            return null;
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, "STRING", tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss.SS"));
        codecRegistry.registerFromTapValue(TapDateValue.class, tapTimeValue -> {
            tapTimeValue.getValue().setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
            return formatTapDateTime(tapTimeValue.getValue(), "yyyy-MM-dd");
        });
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapTimeValue -> {
            tapTimeValue.getValue().setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
            return formatTapDateTime(tapTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSS");
        });

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        //target
        connectorFunctions.supportCreateTable(this::createTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
//        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportWriteRecord(this::writeRecord);


        //source 暂不支持
//        connectorFunctions.supportBatchCount(this::batchCount);
//        connectorFunctions.supportBatchRead(this::batchRead);
//        connectorFunctions.supportStreamRead(this::streamRead);
//        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        //query
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);

        // ddl 暂不支持
//        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
//        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
//        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
//        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> hive1JdbcContext.getConnection(), this::isAlive, c));
    }

    public void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        String sql = "CREATE TABLE IF NOT EXISTS " + hive1Config.getDatabase() + "." + tapTable.getId() + "(" + Hive1JDBCSqlMaker.buildColumnDefinition(tapTable, true);
        StringBuilder clusterBySB = new StringBuilder();
        if (EmptyKit.isNotEmpty(primaryKeys)) {
            for (String field : primaryKeys) {
                String escapeFieldStr = "`" + field + "`";
                clusterBySB.append(escapeFieldStr).append(",");
            }
        } else {
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();

            List<Map.Entry<String, TapField>> nameFields = nameFieldMap.entrySet().stream().sorted(Comparator.comparing(v ->
                    EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).collect(Collectors.toList());


            for (int i = 0; i < nameFields.size(); i++) {
                Map.Entry<String, TapField> tapFieldEntry = nameFields.get(i);
                TapField field = tapFieldEntry.getValue();
                String escapeFieldStr = "`" + field.getName() + "`";
                clusterBySB.append(escapeFieldStr);
                if (i < (nameFields.size() - 1)) {
                    clusterBySB.append(",");
                }
            }
            StringUtils.removeEnd(sql, ",");
        }

        String clusterStr = StringUtils.removeEnd(clusterBySB.toString(), ",");
        StringBuilder sb = new StringBuilder();
        sb.append("\n)");
        sb.append("\nCLUSTERED BY (" + clusterStr + ") INTO 2 BUCKETS STORED AS ORC \nTBLPROPERTIES ('transactional'='true')");
        sql = sql + sb.toString();
        try {
            List<String> sqls = TapSimplify.list();
            sqls.add(sql);
            TapLogger.info("table 为:", "table->{}", tapTable.getId());
            hive1JdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
    }

    public void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            hive1JdbcContext.execute("DROP TABLE IF EXISTS " + hive1Config.getDatabase() + "." + tapDropTableEvent.getTableId());
        } catch (SQLException e) {
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }

    }

    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) {
        List<String> sqls = fieldDDLHandlers.handle(tapFieldBaseEvent, tapConnectorContext);
        if (null == sqls) {
            return;
        }
        for (String sql : sqls) {
            try {
                TapLogger.info(TAG, "Execute ddl sql: " + sql);
                hive1JdbcContext.execute(sql);
            } catch (SQLException e) {
                throw new RuntimeException("Execute ddl sql failed: " + sql + ", error: " + e.getMessage(), e);
            }
        }
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        TapLogger.info("线程debug", "writeRecord当前线程为:{},tapRecordEvents个数:{}", Thread.currentThread().getName(), tapRecordEvents.size());
        WriteListResult<TapRecordEvent> writeListResult = this.hive1Writer.write(tapConnectorContext, tapTable, tapRecordEvents);
        consumer.accept(writeListResult);
    }

    //需要改写成ck的创建索引方式
    private void createIndex(TapConnectorContext connectorContext, TapTable tapTable, TapCreateIndexEvent createIndexEvent) {
        try {
            List<String> sqls = TapSimplify.list();
            if (EmptyKit.isNotEmpty(createIndexEvent.getIndexList())) {
                createIndexEvent.getIndexList().stream().filter(i -> !i.isPrimary()).forEach(i ->
                        sqls.add("CREATE " + (i.isUnique() ? "UNIQUE " : " ") + "INDEX " +
                                (EmptyKit.isNotNull(i.getName()) ? "IF NOT EXISTS \"" + i.getName() + "\"" : "") + " ON \"" + hive1Config.getDatabase() + "\".\"" + tapTable.getId() + "\"(" +
                                i.getIndexFields().stream().map(f -> "\"" + f.getName() + "\" " + (f.getFieldAsc() ? "ASC" : "DESC"))
                                        .collect(Collectors.joining(",")) + ')'));
            }
            hive1JdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Indexes for " + tapTable.getId() + " Failed! " + e.getMessage());
        }

    }

    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = "SELECT * FROM " + hive1Config.getDatabase() + "." + table.getId() + " " + Hive1JDBCSqlMaker.buildSqlByAdvanceFilter(filter);
        try {
            hive1JdbcContext.query(sql, resultSet -> {
                FilterResults filterResults = new FilterResults();
                while (resultSet != null && resultSet.next()) {
                    filterResults.add(DbKit.getRowFromResultSet(resultSet, DbKit.getColumnsFromResultSet(resultSet)));
                    if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                        consumer.accept(filterResults);
                        filterResults = new FilterResults();
                    }
                }
                if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                    filterResults.getResults().stream().forEach(l -> l.entrySet().forEach(v -> {
                        if (v.getValue() instanceof String) {
                            v.setValue(((String) v.getValue()).trim());
                        }
                    }));
                    consumer.accept(filterResults);
                }
            });
        } catch (Throwable e) {
            TapLogger.error(TAG, "query error:{}", e.getMessage());
        }
    }


    // 不支持偏移量
    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql = "SELECT * FROM " + hive1Config.getDatabase() + "." + tapTable.getId() + "\"";
        hive1JdbcContext.query(sql, resultSet -> {
            List<TapEvent> tapEvents = list();
            //get all column names
            List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
            while (isAlive() && resultSet.next()) {
                DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                for (Map.Entry<String, Object> e : dataMap.entrySet()) {
                    Object value = e.getValue();
                    if (value instanceof String) {
                        e.setValue(((java.lang.String) value).trim());
                    }
                }
                tapEvents.add(insertRecordEvent(dataMap, tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    eventsOffsetConsumer.accept(tapEvents, null);
                    tapEvents = list();
                }
            }
            eventsOffsetConsumer.accept(tapEvents, null);
        });

    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        AtomicLong count = new AtomicLong(0);
        String sql = "SELECT COUNT(1) FROM " + hive1Config.getDatabase() + "." + tapTable.getId() + "\"";
        hive1JdbcContext.queryWithNext(sql, resultSet -> count.set(resultSet.getLong(1)));
        return count.get();
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        try {
            if (hive1JdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() == 1) {
                hive1JdbcContext.execute("TRUNCATE TABLE " + hive1Config.getDatabase() + "." + tapClearTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }


    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        hive1Config = (Hive1Config) new Hive1Config().load(connectionContext.getConnectionConfig());
        try (
                Hive1Test hive1Test = new Hive1Test(hive1Config, consumer)
        ) {
            hive1Test.testOneByOne();
        }
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return hive1Writer.tableCount(connectionContext);
    }
}
