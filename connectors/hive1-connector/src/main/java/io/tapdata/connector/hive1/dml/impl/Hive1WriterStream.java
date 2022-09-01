package io.tapdata.connector.hive1.dml.impl;

import io.tapdata.connector.hive1.Hive1RawFileSystem;
import io.tapdata.connector.hive1.config.Hive1Config;
import io.tapdata.connector.hive1.dml.Hive1Writer;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hive.hcatalog.streaming.*;
import org.apache.thrift.TException;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Hive1WriterStream implements Hive1Writer {

    public static final String TAG = Hive1WriterStream.class.getSimpleName();

    private HiveConf hiveConf;
    private IMetaStoreClient metaStoreClient;

    private Hive1Config hive1Config;

    private AtomicBoolean running = new AtomicBoolean(true);

    private final Map<String, Table> tableCache = new ConcurrentHashMap<>();
    private final Map<String, List<String>> tableFieldsCache = new ConcurrentHashMap<>();
    private final Map<String, HiveEndPoint> hiveEndPointCache = new ConcurrentHashMap<>();
    private final Map<String, StreamingConnection> hiveStreamingConnCache = new ConcurrentHashMap<>();
    private final Map<String, DelimitedInputWriter> hiveDelimitedWriterCache = new ConcurrentHashMap<>();


    public Hive1WriterStream(Hive1Config hive1Config) {
        try {
            this.hive1Config = hive1Config;
            this.hiveConf = newHiveConf();
            metaStoreClient = new HiveMetaStoreClient(this.hiveConf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private HiveConf newHiveConf() throws Exception {
        HiveConf conf = new HiveConf(this.getClass());
        conf.set("fs.raw.impl", Hive1RawFileSystem.class.getName());
        conf.setVar(HiveConf.ConfVars.METASTOREURIS, getMetastoreUri());
        conf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, true);
        conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);

        // prepare transaction database
        TxnDbUtil.setConfValues(conf);
//        TxnDbUtil.cleanDb();
//        TxnDbUtil.prepDb();
//
        return conf;
    }

    private String getMetastoreUri() {
        return String.format("thrift://%s:%d", hive1Config.getHost(), hive1Config.getPort());
    }


    @Override
    public WriteListResult<TapRecordEvent> write(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        return batchInsert(tapConnectorContext, tapTable, tapRecordEvents);
    }

    public void onDestroy() {
        this.running.set(false);
        this.tableFieldsCache.clear();
        this.hiveEndPointCache.clear();
        this.hiveStreamingConnCache.clear();
        this.hiveDelimitedWriterCache.clear();
    }

    public WriteListResult<TapRecordEvent> batchInsert(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEventList) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        String insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        if (null != tapConnectorContext.getConnectorCapabilities()
                && null != tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY)) {
            insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        }
        if (
                CollectionUtils.isEmpty(tapTable.primaryKeys())
                        && (CollectionUtils.isEmpty(tapTable.getIndexList()) || null == tapTable.getIndexList().stream().filter(TapIndex::isUnique).findFirst().orElse(null))
                        && ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(insertDmlPolicy)
        ) {
            return writeListResult;
        }
        String tableName = tapTable.getId();
        Table table = tableCache.get(tableName);
        if (table == null) {
            table = metaStoreClient.getTable(hive1Config.getDatabase(), tapTable.getId());
            tableCache.put(tableName, table);
        }
        List<String> fieldNames = tableFieldsCache.get(tableName);
        if (fieldNames == null) {
            fieldNames = new ArrayList<>();
            for (FieldSchema fieldSchema : table.getPartitionKeys()) {
                fieldNames.add(fieldSchema.getName());
            }
            for (FieldSchema fieldSchema : table.getSd().getCols()) {
                fieldNames.add(fieldSchema.getName());
            }
            tableFieldsCache.put(tableName, fieldNames);
        }
        HiveEndPoint hiveEndPoint = hiveEndPointCache.get(tableName);
        if (hiveEndPoint == null) {
            hiveEndPoint = new HiveEndPoint(getMetastoreUri().toLowerCase(), hive1Config.getDatabase(), tapTable.getId(), null);
            hiveEndPointCache.put(tableName, hiveEndPoint);
        }
        StreamingConnection streamingConnection = hiveStreamingConnCache.getOrDefault(tableName, null);
        if (streamingConnection == null) {
            streamingConnection = hiveEndPoint.newConnection(true);
            hiveStreamingConnCache.put(tableName, streamingConnection);
        }
        DelimitedInputWriter writer = hiveDelimitedWriterCache.getOrDefault(tableName, null);
        if (writer == null) {
            writer = new DelimitedInputWriter(fieldNames.toArray(new String[0]), ",", hiveEndPoint, hiveConf);
            hiveDelimitedWriterCache.put(tableName, writer);
        }

        TransactionBatch txnBatch = null;
        try {
            List<String> rows = new ArrayList<>();

            for (TapRecordEvent tapRecordEvent : tapRecordEventList) {
                List<Object> values = new ArrayList<>();
                // hive is case-insensitive, should use lower case
                Map<String, Object> after = lowercaseMapKey(((TapInsertRecordEvent) tapRecordEvent).getAfter());
                for (String fieldName : fieldNames) {
                    values.add(after.getOrDefault(fieldName.toLowerCase(), ""));
                }
                rows.add(StringUtils.join(values, ","));
            }
            try {
                streamingConnection.fetchTransactionBatch(rows.size(), writer);
            } catch (Exception e) {
                TapLogger.info(TAG, "former connection was disconnected, here we reconnect");
                streamingConnection = hiveEndPoint.newConnection(true);
                hiveStreamingConnCache.put(tableName, streamingConnection);
                writer = new DelimitedInputWriter(fieldNames.toArray(new String[0]), ",", hiveEndPoint);
                hiveDelimitedWriterCache.put(tableName, writer);
                txnBatch = streamingConnection.fetchTransactionBatch(rows.size(), writer);
            }
            txnBatch.beginNextTransaction();
            for (String row : rows) {
                txnBatch.write(row.getBytes(StandardCharsets.UTF_8));
            }
            writeListResult.incrementInserted(rows.size());
            txnBatch.commit();
        } catch (Exception e) {
            if (txnBatch != null) txnBatch.abort();
            TapLogger.error(TAG, "hive streaming batch insert error:{}", e.getMessage());
        } finally {
            if (txnBatch != null) txnBatch.close();
        }
        return writeListResult;
    }


    private Map<String, Object> lowercaseMapKey(Map<String, Object> origin) {
        Map<String, Object> map = new HashMap<>();
        for (String key : origin.keySet()) {
            map.put(key.toLowerCase(), origin.get(key));
        }
        return map;
    }


//    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
//
//        // load schema
//        String databaseName = hive1Config.getDatabase();
//        List<RelateDataBaseTable> tables = new ArrayList<>();
//        try {
//            List<String> tableNames = metaStoreClient.getAllTables(databaseName);
//            int idx = 0;
//            for (String tableName : tableNames) {
//                idx += 1;
//                // generate relate database table
//                RelateDataBaseTable table = new RelateDataBaseTable();
//                table.setTable_name(tableName);
//                // generate relate database fields
//                List<RelateDatabaseField> fields = new ArrayList<>();
//                List<FieldSchema> hiveFields = new ArrayList<>();
//
//
//                Table tbl = metaStoreClient.getTable(databaseName, tableName);
//                hiveFields.addAll(tbl.getSd().getCols());
//                // partition column should also be seen as RelateDatabaseField
//                hiveFields.addAll(tbl.getPartitionKeys());
//                for (FieldSchema hiveField : hiveFields) {
//                    RelateDatabaseField field = new RelateDatabaseField();
//                    field.setField_name(hiveField.getName());
//                    String data_type = hiveField.getType().toUpperCase().split("\\(")[0];
//                    field.setData_type(data_type);
//                    // the `not null` and `default` constraints, hive support these constraints after 3.0.0, skip these settings
//                    // See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Constraints
//                    fields.add(field);
//                }
//                table.setFields(fields);
//                if (connections.isLoadSchemaField() && null != connections.getTableConsumer()) {
//                    Consumer<RelateDataBaseTable> consumer = connections.getTableConsumer();
//                    consumer.accept(table);
//                    if (idx == tableNames.size()) {
//                        consumer.accept(new RelateDataBaseTable(true));
//                    }
//                }
//                tables.add(table);
//            }
//            result.setSchema(tables);
//        } catch (Exception e) {
//            e.printStackTrace();
//            result.setErrMessage("failed to load schema: " + e.getMessage());
//        }
//
//        return result;
//    }

    @Override
    public void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        try {
            TapTable tapTable = tapCreateTableEvent.getTable();
            if (metaStoreClient.tableExists(hive1Config.getDatabase(), tapTable.getId())) {
                TapLogger.info(TAG, "Table {} is exists,no need to create", tapTable.getId());
                return;
            }
            Database db = metaStoreClient.getDatabase(hive1Config.getDatabase());
            Table table = new Table();
            table.setDbName(hive1Config.getDatabase());
            table.setTableName(tapTable.getId());
            String locationUri = db.getLocationUri();

            // this.sd = new StorageDescriptor(other.sd)
//            table.getSd().setLocation(db.getLocationUri() + Path.SEPARATOR + tapTable.getId());
//            table.getSd().getSerdeInfo().setName(tapTable.getId());
            metaStoreClient.createTable(table);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            metaStoreClient.dropTable(hive1Config.getDatabase(), tapDropTableEvent.getTableId(), true, true);
        } catch (TException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws TException {
        return queryAllTables().size();

    }

    public List<String> queryAllTables() throws TException {
        return metaStoreClient.getAllTables(hive1Config.getDatabase());
    }
}
