package io.tapdata.connector.tablestore;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TimeseriesClient;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.timeseries.ListTimeseriesTableResponse;
import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;

import java.util.*;
import java.util.function.Consumer;

@TapConnectorClass("spec_tablestore.json")
public class TablestoreConnector extends ConnectorBase {

    private TablestoreConfig tablestoreConfig;

    private SyncClient client;

    private TimeseriesClient timeseriesClient;

    private static final String TAG = TablestoreConnector.class.getSimpleName();

    private void initConnection(TapConnectionContext connectorContext) {
        tablestoreConfig = new TablestoreConfig().load(connectorContext.getConnectionConfig());
        if ("NORMAL".equals(tablestoreConfig.getClientType())) {
            client = createInternalClient();
        } else if ("TIMESERIES".equals(tablestoreConfig.getClientType())) {
            timeseriesClient = createTimeseriesClient();
        }
    }

    private TimeseriesClient createTimeseriesClient() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setConnectionTimeoutInMillisecond(5000);
        clientConfiguration.setSocketTimeoutInMillisecond(5000);
        clientConfiguration.setRetryStrategy(new AlwaysRetryStrategy());
        if (Objects.isNull(tablestoreConfig.getAccessKeyToken())) {
            return new TimeseriesClient(tablestoreConfig.getEndpoint(), tablestoreConfig.getAccessKeyId(),
                    tablestoreConfig.getAccessKeySecret(), tablestoreConfig.getInstance(), clientConfiguration);
        } else {
            return new TimeseriesClient(tablestoreConfig.getEndpoint(), tablestoreConfig.getAccessKeyId(),
                    tablestoreConfig.getAccessKeySecret(), tablestoreConfig.getInstance(), clientConfiguration, tablestoreConfig.getAccessKeyToken());
        }
    }

    private SyncClient createInternalClient() {
        if (Objects.isNull(tablestoreConfig.getAccessKeyToken())) {
            return new SyncClient(tablestoreConfig.getEndpoint(), tablestoreConfig.getAccessKeyId(), tablestoreConfig.getAccessKeySecret(),
                    tablestoreConfig.getInstance());
        } else {
            return new SyncClient(tablestoreConfig.getEndpoint(), tablestoreConfig.getAccessKeyId(), tablestoreConfig.getAccessKeySecret(),
                    tablestoreConfig.getInstance(), tablestoreConfig.getAccessKeyToken());
        }
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        initConnection(connectionContext);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        if ("NORMAL".equals(tablestoreConfig.getClientType())) {
            client.shutdown();
        } else if ("TIMESERIES".equals(tablestoreConfig.getClientType())) {
            timeseriesClient.shutdown();
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);

        codecRegistry.registerFromTapValue(TapRawValue.class, ColumnType.STRING.name(), tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return tapRawValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, ColumnType.STRING.name(), TapArrayValue -> {
            if (TapArrayValue != null && TapArrayValue.getValue() != null) return TapArrayValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, ColumnType.STRING.name(), tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, ColumnType.STRING.name(), tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, ColumnType.STRING.name(), tapDateTimeValue -> formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS"));
        codecRegistry.registerFromTapValue(TapDateValue.class, ColumnType.STRING.name(), tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        if ("NORMAL".equals(tablestoreConfig.getClientType())) {
            ListTableResponse listTableResponse = client.listTable();
            if (Objects.nonNull(listTableResponse) && EmptyKit.isNotEmpty(listTableResponse.getTableNames())) {
                List<List<String>> partition = Lists.partition(listTableResponse.getTableNames(), tableSize);
                for (List<String> tableNames : partition) {
                    List<TapTable> tapTableList = list();
                    for (String tableName : tableNames) {
                        DescribeTableResponse describeTableResponse = client.describeTable(new DescribeTableRequest(tableName));
                        TableMeta tableMeta = describeTableResponse.getTableMeta();

                        TapTable table = table(tableName);
                        if (!tableMeta.getPrimaryKeyMap().isEmpty()) {
                            for (Map.Entry<String, PrimaryKeyType> entry : tableMeta.getPrimaryKeyMap().entrySet()) {
                                TapField tapField = new TapField(entry.getKey(), entry.getValue().name());
                                table.add(tapField);
                            }
                        }
                        if (!tableMeta.getDefinedColumnMap().isEmpty()) {
                            for (Map.Entry<String, DefinedColumnType> entry : tableMeta.getDefinedColumnMap().entrySet()) {
                                TapField tapField = new TapField(entry.getKey(), entry.getValue().name());
                                table.add(tapField);
                            }
                        }

                        List<IndexMeta> indexMeta = describeTableResponse.getIndexMeta();
                        List<TapIndex> tapIndexList = new ArrayList<>();
                        for (IndexMeta meta : indexMeta) {
                            TapIndex tapIndex = new TapIndex();
                            tapIndex.setName(meta.getIndexName());

                            List<TapIndexField> indexFields = Lists.newLinkedList();
                            if (EmptyKit.isNotEmpty(meta.getPrimaryKeyList())) {
                                for (String k : meta.getPrimaryKeyList()) {
                                    TapIndexField tapIndexField = new TapIndexField();
                                    tapIndexField.setName(k);
                                    tapIndexField.setFieldAsc(true);
                                    indexFields.add(tapIndexField);
                                }
                            }
                            if (EmptyKit.isNotEmpty(meta.getDefinedColumnsList())) {
                                for (String k : meta.getDefinedColumnsList()) {
                                    TapIndexField tapIndexField = new TapIndexField();
                                    tapIndexField.setName(k);
                                    tapIndexField.setFieldAsc(true);
                                    indexFields.add(tapIndexField);
                                }
                            }

                            if (EmptyKit.isNotEmpty(indexFields)) {
                                tapIndex.setIndexFields(indexFields);
                            }
                            tapIndexList.add(tapIndex);
                        }
                        if (EmptyKit.isNotEmpty(tapIndexList)) {
                            table.setIndexList(tapIndexList);
                        }

                        tapTableList.add(table);
                    }

                    consumer.accept(tapTableList);
                }
            }
        } else if ("TIMESERIES".equals(tablestoreConfig.getClientType())) {

        }
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        tablestoreConfig = new TablestoreConfig().load(connectionContext.getConnectionConfig());
        TestItem testConnect;
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        SyncClient syncClient = null;
        TimeseriesClient timeseriesClientTemp = null;
        try {
            connectionOptions.connectionString(tablestoreConfig.getConnectionString());
            if ("NORMAL".equals(tablestoreConfig.getClientType())) {
                syncClient = createInternalClient();
                syncClient.listTable();
                testConnect = testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
            } else if ("TIMESERIES".equals(tablestoreConfig.getClientType())) {
                timeseriesClientTemp = createTimeseriesClient();
                timeseriesClientTemp.listTimeseriesTable();
                testConnect = testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
            } else {
                testConnect = testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Elasticsearch client ping failed!");
            }
            consumer.accept(testConnect);
            return connectionOptions;
        } catch (Exception e) {
            testConnect = testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
            consumer.accept(testConnect);
            return connectionOptions;
        } finally {
            if (Objects.nonNull(syncClient)) {
                syncClient.shutdown();
            }
            if (Objects.nonNull(timeseriesClientTemp)) {
                timeseriesClientTemp.shutdown();
            }
        }

    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        if ("NORMAL".equals(tablestoreConfig.getClientType())) {
            ListTableResponse listTableResponse = client.listTable();
            if (Objects.nonNull(listTableResponse) && EmptyKit.isNotEmpty(listTableResponse.getTableNames())) {
                return listTableResponse.getTableNames().size();
            }
        } else if ("TIMESERIES".equals(tablestoreConfig.getClientType())) {
            ListTimeseriesTableResponse listTimeseriesTableResponse = timeseriesClient.listTimeseriesTable();
            if (Objects.nonNull(listTimeseriesTableResponse) && Objects.nonNull(listTimeseriesTableResponse.getTimeseriesTableNames())) {
                return listTimeseriesTableResponse.getTimeseriesTableNames().size();
            }
        }
        return 0;
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable, Exception {
        String tableId = tapTable.getId();
        if ("NORMAL".equals(tablestoreConfig.getClientType())) {
            DescribeTableRequest request = new DescribeTableRequest(tableId);
            DescribeTableResponse response = client.describeTable(request);
            TableMeta tableMeta = response.getTableMeta();
            if (Objects.nonNull(tableMeta)) {
                Set<String> primaryKeySet = tableMeta.getPrimaryKeyMap().keySet();

                WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
                long insertCount = 0L;
                long updateCount = 0L;
                long deleteCount = 0L;
                for (TapRecordEvent recordEvent : tapRecordEvents) {
                    if (recordEvent instanceof TapInsertRecordEvent) {
                        try {
                            Map<String, Object> after = ((TapInsertRecordEvent) recordEvent).getAfter();

                            HashSet<String> resSet = new HashSet<>(primaryKeySet);
                            RowPutChange putChange;
                            resSet.retainAll(after.keySet());
                            if (EmptyKit.isNotEmpty(resSet)) {
                                PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                                for (String k : resSet) {
                                    ColumnType columnType = ColumnType.valueOf(
                                            Optional.ofNullable(tableMeta.getPrimaryKeyMap().get(k))
                                                    .orElseThrow(()-> new RuntimeException("table primaryKeyMap not find "+k))
                                                    .name());
                                    Object value = transferValueType(columnType, after.get(k));
                                    pkBuilder.addPrimaryKeyColumn(k, PrimaryKeyValue.fromColumn(new ColumnValue(value, columnType)));
                                }
                                putChange = new RowPutChange(tableId, pkBuilder.build());
                            } else {
                                putChange = new RowPutChange(tableId);
                            }

                            for (Map.Entry<String, Object> entry : after.entrySet()) {
                                String fieldName = entry.getKey();
                                if (resSet.contains(fieldName)) {
                                    continue;
                                }

                                ColumnType columnType = ColumnType.valueOf(
                                        Optional.ofNullable(tableMeta.getDefinedColumnMap().get(fieldName))
                                        .orElseThrow(() -> new RuntimeException("table definedColumnMap not find "+fieldName))
                                                .name());
                                Object value = transferValueType(columnType, entry.getValue());
                                putChange.addColumn(fieldName, new ColumnValue(value, columnType));
                            }
                            client.putRow(new PutRowRequest(putChange));
                            insertCount++;
                        } catch (Exception e) {
                            listResult.addError(recordEvent, e);
                        }

                    } else if (recordEvent instanceof TapUpdateRecordEvent) {
                        try {
                            Map<String, Object> after = ((TapUpdateRecordEvent) recordEvent).getAfter();

                            HashSet<String> resSet = new HashSet<>(primaryKeySet);
                            RowUpdateChange updateChange;
                            resSet.retainAll(after.keySet());
                            if (EmptyKit.isNotEmpty(resSet)) {
                                PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                                for (String k : resSet) {
                                    ColumnType columnType = ColumnType.valueOf(
                                            Optional.ofNullable(tableMeta.getPrimaryKeyMap().get(k))
                                                    .orElseThrow(()-> new RuntimeException("table primaryKeyMap not find "+k))
                                                    .name());
                                    Object value = transferValueType(columnType, after.get(k));
                                    pkBuilder.addPrimaryKeyColumn(k, PrimaryKeyValue.fromColumn(new ColumnValue(value, columnType)));
                                }
                                updateChange = new RowUpdateChange(tableId, pkBuilder.build());
                            } else {
                                updateChange = new RowUpdateChange(tableId);
                            }

                            for (Map.Entry<String, Object> entry : after.entrySet()) {
                                String fieldName = entry.getKey();
                                if (resSet.contains(fieldName)) {
                                    continue;
                                }
                                ColumnType columnType = ColumnType.valueOf(
                                        Optional.ofNullable(tableMeta.getDefinedColumnMap().get(fieldName))
                                                .orElseThrow(() -> new RuntimeException("table definedColumnMap not find "+fieldName))
                                                .name());
                                Object value = transferValueType(columnType, entry.getValue());
                                updateChange.put(fieldName, new ColumnValue(value, columnType));
                            }

                            client.updateRow(new UpdateRowRequest(updateChange));
                            updateCount++;
                        }  catch (Exception e) {
                            listResult.addError(recordEvent, e);
                        }
                    } else if (recordEvent instanceof TapDeleteRecordEvent) {
                        try {
                            Map<String, Object> before = ((TapDeleteRecordEvent) recordEvent).getBefore();
                            RowDeleteChange deleteChange;
                            HashSet<String> resSet = new HashSet<>(primaryKeySet);
                            resSet.retainAll(before.keySet());
                            if (EmptyKit.isNotEmpty(resSet)) {
                                PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                                for (String k : resSet) {
                                    ColumnType columnType = ColumnType.valueOf(
                                            Optional.ofNullable(tableMeta.getPrimaryKeyMap().get(k))
                                                    .orElseThrow(()-> new RuntimeException("table primaryKeyMap not find "+k))
                                                    .name());
                                    Object value = transferValueType(columnType, before.get(k));
                                    pkBuilder.addPrimaryKeyColumn(k, PrimaryKeyValue.fromColumn(new ColumnValue(value, columnType)));
                                }
                                deleteChange = new RowDeleteChange(tableId, pkBuilder.build());
                            } else {
                                deleteChange = new RowDeleteChange(tableId);
                            }

                            client.deleteRow(new DeleteRowRequest(deleteChange));
                            deleteCount++;
                        }  catch (Exception e) {
                            listResult.addError(recordEvent, e);
                        }
                    }
                }

                writeListResultConsumer.accept(listResult
                        .insertedCount(insertCount)
                        .modifiedCount(updateCount)
                        .removedCount(deleteCount));
            }
        } else if ("TIMESERIES".equals(tablestoreConfig.getClientType())) {

        }
    }

    private Object transferValueType(ColumnType columnType, Object value) {
        if (ColumnType.INTEGER.equals(columnType)) {
            value = Objects.isNull(value) ? 0L : Long.parseLong(value.toString());
        } else if (ColumnType.DOUBLE.equals(columnType)) {
            value = Objects.isNull(value) ? 0 :  Double.parseDouble(value.toString());
        } else if (ColumnType.STRING.equals(columnType)) {
            value = Objects.isNull(value) ? "" :  value;
        }
        return value;
    }

    private CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws Throwable {
        CreateTableOptions createTableOptions = new CreateTableOptions();
        TapTable tapTable = tapCreateTableEvent.getTable();
        if ("NORMAL".equals(tablestoreConfig.getClientType())) {
            ListTableResponse listTableResponse = client.listTable();
            if (Objects.nonNull(listTableResponse) && !listTableResponse.getTableNames().contains(tapTable.getId())) {
                TableMeta tableMeta = new TableMeta(tapTable.getId());

                Collection<String> primaryKeyList = tapTable.primaryKeys(true);

                if (EmptyKit.isEmpty(primaryKeyList) || primaryKeyList.size() > 4) {
                    throw new Exception("create table error, The primary key of the data table must be specified when creating the data table. The primary key contains 1 to 4 primary key columns, each of which has a name and type.");
                }

                for (TapField field : tapTable.getNameFieldMap().values()) {
                    String dataType = field.getDataType();
                    if (primaryKeyList.contains(field.getName())) {
                        tableMeta.addPrimaryKeyColumn(field.getName(), PrimaryKeyType.valueOf(dataType));
                    } else {
                        tableMeta.addDefinedColumn(field.getName(), DefinedColumnType.valueOf(dataType));
                    }
                }

                int timeToLive = -1;
                int maxVersions = 1;
                TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

                ArrayList<IndexMeta> indexMetas = new ArrayList<>();
                if (Objects.nonNull(tapTable.getIndexList())) {
                    for (TapIndex index : tapTable.getIndexList()) {
                        IndexMeta indexMeta = new IndexMeta(index.getName());
                        for (TapIndexField indexField : index.getIndexFields()) {
                            if (primaryKeyList.contains(indexField.getName())) {
                                indexMeta.addPrimaryKeyColumn(indexField.getName());
                            } else {
                                indexMeta.addDefinedColumn(indexField.getName());
                            }
                        }
                        indexMetas.add(indexMeta);
                    }
                }

                CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions, indexMetas);
                client.createTable(request);
            } else {
                createTableOptions.setTableExists(true);
                return createTableOptions;
            }
        } else if ("TIMESERIES".equals(tablestoreConfig.getClientType())) {

        }

        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws Throwable {
        String tableId = tapClearTableEvent.getTableId();
        if ("NORMAL".equals(tablestoreConfig.getClientType())) {
            ListTableResponse listTableResponse = client.listTable();
            List<String> tableNames = listTableResponse.getTableNames();
            Optional.ofNullable(tableNames).ifPresent(list -> {
                        if (list.contains(tableId)) {
                        }
                    }
            );
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws Throwable {
        String tableId = tapDropTableEvent.getTableId();
        if ("NORMAL".equals(tablestoreConfig.getClientType())) {
            ListTableResponse listTableResponse = client.listTable();
            List<String> tableNames = listTableResponse.getTableNames();
            Optional.ofNullable(tableNames).ifPresent(list -> {
                        if (list.contains(tableId)) {
                            DeleteTableRequest deleteTableRequest = new DeleteTableRequest(tableId);
                            client.deleteTable(deleteTableRequest);
                        }
                    }
            );
        }
    }
}
