package io.tapdata.connector.dws;

import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.dws.bean.DwsTapTable;
import io.tapdata.connector.dws.config.DwsConfig;
import io.tapdata.connector.postgres.PostgresConnector;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.dml.PostgresRecordWriter;
import io.tapdata.connector.postgres.PostgresTest;
import io.tapdata.connector.postgres.bean.PostgresColumn;
import io.tapdata.connector.postgres.cdc.PostgresCdcRunner;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.ddl.PostgresDDLSqlGenerator;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.postgresql.geometric.*;
import org.postgresql.jdbc.PgArray;
import org.postgresql.jdbc.PgSQLXML;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * PDK for Postgresql
 *
 * @author Jarad
 * @date 2022/4/18
 */
@TapConnectorClass("spec_dws.json")
public class DwsConnector extends PostgresConnector {
    protected DwsConfig dwsConfig;
    protected DwsJdbcContext dwsJdbcContext;
    private DwsTest dwsTest;
    protected String postgresVersion;
    private Map<String, DwsTapTable> dwsTapTableMap = new ConcurrentHashMap<>();

    @Override
    public void onStart(TapConnectionContext connectorContext) {
        initConnection(connectorContext);
    }

    protected TapField makeTapField(DataMap dataMap) {
        return new PostgresColumn(dataMap).getTapField();
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        dwsConfig = (DwsConfig) new DwsConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(dwsConfig.getConnectionString());
        try (
                DwsTest dwsTest = new DwsTest(dwsConfig, consumer).initContext()
        ) {
            dwsTest.testOneByOne();
            return connectionOptions;
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        //test
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        // target
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateIndex(this::createIndex);
//        connectorFunctions.supportQueryIndexes(this::queryIndexes);
//        connectorFunctions.supportDeleteIndex(this::dropIndexes);
        // query
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset);
        // ddl
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> postgresJdbcContext.getConnection(), this::isAlive, c));
        connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);

        codecRegistry.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "text", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });

        codecRegistry.registerToTapValue(PgArray.class, (value, tapType) -> {
            PgArray pgArray = (PgArray) value;
            try (
                    ResultSet resultSet = pgArray.getResultSet()
            ) {
                return new TapArrayValue(DbKit.getDataArrayByColumnName(resultSet, "VALUE"));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        codecRegistry.registerToTapValue(PgSQLXML.class, (value, tapType) -> {
            try {
                return new TapStringValue(((PgSQLXML) value).getString());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        codecRegistry.registerToTapValue(PGbox.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGcircle.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGline.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGlseg.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpath.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGobject.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpoint.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpolygon.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(UUID.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGInterval.class, (value, tapType) -> {
            //P1Y1M1DT12H12M12.312312S
            PGInterval pgInterval = (PGInterval) value;
            String interval = "P" + pgInterval.getYears() + "Y" +
                    pgInterval.getMonths() + "M" +
                    pgInterval.getDays() + "DT" +
                    pgInterval.getHours() + "H" +
                    pgInterval.getMinutes() + "M" +
                    pgInterval.getSeconds() + "S";
            return new TapStringValue(interval);
        });
        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
        codecRegistry.registerFromTapValue(TapYearValue.class, "character(4)", tapYearValue -> formatTapDateTime(tapYearValue.getValue(), "yyyy"));
        connectorFunctions.supportGetTableInfoFunction(this::getTableInfo);
        connectorFunctions.supportTransactionBeginFunction(this::beginTransaction);
        connectorFunctions.supportTransactionCommitFunction(this::commitTransaction);
        connectorFunctions.supportTransactionRollbackFunction(this::rollbackTransaction);
    }

    @Override
    protected CreateTableOptions createTableV2(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws SQLException {
        TapTable table = createTableEvent.getTable();
        Collection<String> primaryKeys = table.primaryKeys();
        TapField distributeKey=null;
        if(primaryKeys.isEmpty()){
            distributeKey = table.getNameFieldMap().values().iterator().next();
        }else{
            String firstPk = primaryKeys.iterator().next();
            distributeKey = table.getNameFieldMap().get(firstPk);
        }
        Pattern pattern = Pattern.compile("nvarchar2\\(\\d+\\)",Pattern.CASE_INSENSITIVE);
        if(pattern.matcher(distributeKey.getDataType()).matches()){
            distributeKey.setDataType("nvarchar2");
        }
        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        for (Map.Entry<String, TapField> tapFieldEntry : nameFieldMap.entrySet()) {
            if (tapFieldEntry.equals(distributeKey.getName())){
                tapFieldEntry.setValue(distributeKey);
                break;
            }
        }
        table.setNameFieldMap(nameFieldMap);
        createTableEvent.setTable(table);
        CreateTableOptions createTableOptions = super.createTableV2(connectorContext, createTableEvent);
        createUniqueIndexForLogicPkIfNeed(connectorContext, table);
        return createTableOptions;
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        EmptyKit.closeQuietly(dwsTest);
        EmptyKit.closeQuietly(dwsJdbcContext);
    }

    //initialize jdbc context, slot name, version
    private void initConnection(TapConnectionContext connectionContext) {
        dwsConfig = (DwsConfig) new DwsConfig().load(connectionContext.getConnectionConfig());
        dwsTest = new DwsTest(dwsConfig, testItem -> {
        }).initContext();
        dwsJdbcContext = new DwsJdbcContext(dwsConfig);
        commonDbConfig = dwsConfig;
        jdbcContext = dwsJdbcContext;
        commonSqlMaker = new CommonSqlMaker();
        postgresVersion = dwsJdbcContext.queryVersion();
        ddlSqlGenerator = new PostgresDDLSqlGenerator();
        tapLogger = connectionContext.getLog();
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
        exceptionCollector = new PostgresExceptionCollector();
    }

    //write records as all events, prepared
    protected void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        String insertDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        AtomicReference<DwsTapTable> dwsTapTable = new AtomicReference<>();
        if (dwsTapTableMap.containsKey(tapTable.getId())) {
            dwsTapTable.set(dwsTapTableMap.get(tapTable.getId()));
        } else {
            boolean isPartition = discoverPartition(tapTable.getId());
            List<String> distributedKeys = queryForDistributedKeys(tapTable);
            if (isPartition) {
                List<DataMap> tableMap = dwsJdbcContext.queryAllTables(Collections.singletonList(tapTable.getId()));
                if (!tableMap.isEmpty()) {
                    singleThreadDiscoverSchema(tableMap, tables -> {
                        if (null != tables && tables.size() >= 1) {
                            TapTable loadedTapTable = tables.get(0);
                            dwsTapTable.set(new DwsTapTable(loadedTapTable, isPartition, distributedKeys));
                        }
                    });
                }
            } else {
                dwsTapTable.set(new DwsTapTable(tapTable, false, distributedKeys));
            }
            dwsTapTableMap.put(tapTable.getId(), dwsTapTable.get());
            connectorContext.getLog().info("DwsTapTableMap has been loaded successfully,tapTable:{},isPartition:{},getDistributedKeys:{}",
                    dwsTapTableMap.get(tapTable.getId()).getTapTable(),
                    dwsTapTableMap.get(tapTable.getId()).isPartition(),dwsTapTableMap.get(tapTable.getId()).getDistributedKeys());
        }

        if (isTransaction) {
            String threadName = Thread.currentThread().getName();
            Connection connection;
            if (transactionConnectionMap.containsKey(threadName)) {
                connection = transactionConnectionMap.get(threadName);
            } else {
                connection = dwsJdbcContext.getConnection();
                transactionConnectionMap.put(threadName, connection);
            }
            new DwsRecordWriter(dwsJdbcContext, connection, dwsTapTable.get())
                    .setVersion(postgresVersion)
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .write(tapRecordEvents, writeListResultConsumer, this::isAlive);

        } else {
            new DwsRecordWriter(dwsJdbcContext, dwsTapTable.get())
                    .setVersion(postgresVersion)
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .write(tapRecordEvents, writeListResultConsumer, this::isAlive);
        }
    }


    protected TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) {
        DataMap dataMap = postgresJdbcContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("size")));
        tableInfo.setStorageSize(new BigDecimal(dataMap.getString("rowcount")).longValue());
        return tableInfo;
    }

    private boolean discoverPartition(String tableName) {
        Integer count;
        count = dwsJdbcContext.queryFromPGPARTITION(Collections.singletonList(tableName));
        return null != count && count > 0;
    }

    private String getCreateIndexForPartitionTableSql(TapTable tapTable, TapIndex tapIndex) {
        StringBuilder sb = new StringBuilder("create ");
        char escapeChar = commonDbConfig.getEscapeChar();
        if (tapIndex.isUnique()) {
            sb.append("unique ");
        }
        sb.append("index ");
        if (EmptyKit.isNotBlank(tapIndex.getName())) {
            sb.append(escapeChar).append(tapIndex.getName()).append(escapeChar);
        } else {
            sb.append(escapeChar).append(DbKit.buildIndexName(tapTable.getId())).append(escapeChar);
        }
        sb.append(" on ").append(getSchemaAndTable(tapTable.getId())).append('(')
                .append(tapIndex.getIndexFields().stream().map(f -> escapeChar + f.getName() + escapeChar)
                        .collect(Collectors.joining(","))).append(") local");
        return sb.toString();
    }
    @Override
    protected void createIndex(TapConnectorContext connectorContext, TapTable tapTable, TapCreateIndexEvent createIndexEvent) throws SQLException {
        //判断是否为分区表  SELECT * FROM PG_PARTITION where relname='tableName'
        if(discoverPartition(tapTable.getName())){
            //分区
            List<String> sqlList = TapSimplify.list();
            List<TapIndex> indexList = createIndexEvent.getIndexList()
                    .stream()
                    .filter(v -> discoverIndex(tapTable.getId())
                            .stream()
                            .noneMatch(i -> DbKit.ignoreCreateIndex(i, v)))
                    .collect(Collectors.toList());
            if (EmptyKit.isNotEmpty(indexList)) {
                indexList.stream().filter(i -> !i.isPrimary()).forEach(i ->
                        sqlList.add(getCreateIndexForPartitionTableSql(tapTable, i)));
            }
            jdbcContext.batchExecute(sqlList);

        }else{
            super.createIndex(connectorContext, tapTable, createIndexEvent);
        }
    }

    protected void createUniqueIndexForLogicPkIfNeed(TapConnectorContext connectorContext, TapTable tapTable) throws SQLException {
        Collection<String> primaryKeys = tapTable.primaryKeys(false);
        if (null != primaryKeys && !primaryKeys.isEmpty()){
            return;
        }
        TapCreateIndexEvent tapCreateIndexEvent = new TapCreateIndexEvent();
        List<String> distributedKeys = queryForDistributedKeys(tapTable);
        boolean partition = discoverPartition(tapTable.getId());
        DwsTapTable dwsTapTable = new DwsTapTable(tapTable, partition, distributedKeys);
        Set<String> conflictKeys = dwsTapTable.buildConflictKeys();
        TapIndex tapIndex = new TapIndex().unique(true);
        tapIndex.setIndexFields(conflictKeys.stream().map(key -> new TapIndexField().name(key).fieldAsc(true)).collect(Collectors.toList()));
        tapCreateIndexEvent.setIndexList(Arrays.asList(tapIndex));
        createIndex(connectorContext, dwsTapTable.getTapTable(), tapCreateIndexEvent);
    }

    protected List<String> queryForDistributedKeys(TapTable tapTable){
        return dwsJdbcContext.queryDistributedKeys(dwsJdbcContext.getConfig().getSchema(),tapTable.getName());
    }
}
