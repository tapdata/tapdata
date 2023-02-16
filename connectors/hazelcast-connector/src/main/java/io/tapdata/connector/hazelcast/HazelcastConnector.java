package io.tapdata.connector.hazelcast;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.memory.NativeOutOfMemoryError;
import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.hazelcast.util.HazelcastClientUtil;
import io.tapdata.connector.hazelcast.util.ObjectKey;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapdata.write.WriteValve;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Author:Skeet
 * Date: 2023/2/7
 **/
@TapConnectorClass("spec_hazelcast.json")
public class HazelcastConnector extends ConnectorBase {
    public static final String TAG = HazelcastConnector.class.getSimpleName();
    private static final String TAPDATA_TABLE_LIST = "tapdataDiscoverSchemaIMaps";
    private HazelcastInstance client;
    private WriteValve valve;


    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        try {
            client = HazelcastClientUtil.getClient(connectionContext);
        } catch (Exception e) {
            e.printStackTrace();
            throw new CoreException("The Hazelcast cluster connection fails: " + e.getMessage(), e);
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        try {
            Optional.ofNullable(this.valve).ifPresent(WriteValve::close);
        } finally {
            HazelcastClientUtil.closeClient(client);
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, (value) -> value.getValue().toDate());
        codecRegistry.registerFromTapValue(TapTimeValue.class, (value) -> value.getValue().toDate());
        codecRegistry.registerFromTapValue(TapDateValue.class, (value) -> value.getValue().toDate());
        codecRegistry.registerFromTapValue(TapYearValue.class, (value) -> value.getValue().toDate());

        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createIMap);
        connectorFunctions.supportClearTable(this::clearIMap);
        connectorFunctions.supportDropTable(this::dropIMap);
        connectorFunctions.supportErrorHandleFunction(this::autoRetry);
        connectorFunctions.supportGetTableNamesFunction(this::getImapNames);
    }
    final Object reconnectLock = new int[0];
    private RetryOptions autoRetry(TapConnectionContext tapConnectionContext, PDKMethod pdkMethod, Throwable throwable) {
        RetryOptions retryOptions = RetryOptions.create().beforeRetryMethod(() -> {
            //Do nothing
            if(client == null || !client.getLifecycleService().isRunning()) {
                synchronized (reconnectLock) {
                    if(client == null || !client.getLifecycleService().isRunning()) {
                        try {
                            if(client != null)
                                client.shutdown();
                        } catch (Throwable ignored) {}
                        try {
                            client = HazelcastClientUtil.getClient(tapConnectionContext);
                        } catch (Exception exception) {
                            exception.printStackTrace();
                            TapLogger.warn(TAG, "Reconnect client failed, {}", exception.getMessage());
                        }
                    }
                }
            }
        });
        if (null != matchThrowable(throwable, NativeOutOfMemoryError.class)) {
            retryOptions.needRetry(false);
        } else {
            retryOptions.needRetry(true);
        }
        return retryOptions;
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

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        try {
            IList<String> iMaps = client.getList(TAPDATA_TABLE_LIST);
            List<String> list = new ArrayList<>();
            if (tables == null || tables.isEmpty()) {
                accept(iMaps.stream().map(ConnectorBase::table).collect(Collectors.toList()), tableSize, consumer);
                return;
            }
            List<TapTable> tapTableList = TapSimplify.list();
            if (iMaps.size() > 0) {
                for (String s : iMaps) {
                    if (!list.contains(s) && tables.contains(s))
                        list.add(s);
                }
                for (String k : list) {
                    tapTableList.add(table(k));
                }
            }
            accept(tapTableList, tableSize, consumer);
        } catch (Exception e) {
            throw new Exception("DiscoverSchema failure: " + e.getMessage(), e);
        } finally {
            client.shutdown();
        }
    }

    private final Object lock = new int[0];

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        if (Objects.isNull(this.valve)) {
            synchronized (lock) {
                if (Objects.isNull(this.valve)) {
                    this.valve = WriteValve.open(
                            1400,
                            2,
                            this::write,
                            writeListResultConsumer
                    );
                }
            }
        }
        this.valve.write(tapRecordEvents, tapTable);
    }

    private void write(Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, List<TapRecordEvent> events, TapTable table) {
        TapLogger.info(TAG, "batch events length is: {}", events.size());
        IMap<String, Object> map = client.getMap(table.getId());
        HashMap<String, Object> tempMap = new HashMap<>();
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        for (TapRecordEvent event : events) {
            if(!isAlive())
                return;
            if (event instanceof TapInsertRecordEvent) {
                final TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) event;
                final Map<String, Object> after = insertRecordEvent.getAfter();
                String keyFromData = ObjectKey.getKeyFromData(null, after, table.primaryKeys(true)).toString();
                tempMap.put(keyFromData, after);
                listResult.incrementInserted(1);
            } else if (event instanceof TapUpdateRecordEvent) {
                final TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) event;
                final Map<String, Object> before = updateRecordEvent.getBefore();
                final Map<String, Object> after = updateRecordEvent.getAfter();
                String keyFromData = ObjectKey.getKeyFromData(before, after, table.primaryKeys(true)).toString();
                Map<String, Object> oldValue = (Map<String, Object>) map.get(keyFromData);
                if (oldValue != null) {
                    oldValue.putAll(after);
                    map.put(keyFromData, oldValue);
                } else {
                    map.put(keyFromData, after);
                }
                listResult.incrementModified(1);
            } else {
                final TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) event;
                final Map<String, Object> before = deleteRecordEvent.getBefore();
                String keyFromData = ObjectKey.getKeyFromData(before, null, table.primaryKeys(true)).toString();
                map.delete(keyFromData);
                listResult.incrementRemove(1);
            }
        }
        if (!tempMap.isEmpty()) {
            map.putAll(tempMap);
            tempMap.clear();
        }
        writeListResultConsumer.accept(listResult);
    }

    private void getImapNames(TapConnectionContext tapConnectionContext, int i, Consumer<List<String>> listConsumer) throws Exception {
        try {
            IList<String> discoverSchemaIMaps = client.getList(TAPDATA_TABLE_LIST);
            List<String> tableList = TapSimplify.list();
            if (!discoverSchemaIMaps.isEmpty()) {
                tableList.addAll(discoverSchemaIMaps);
            }
            if (!tableList.isEmpty()) {
                listConsumer.accept(tableList);
                tableList.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("getImapNames failed." + e.getMessage(), e);
        }

    }

    private void dropIMap(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            if (tapDropTableEvent.getTableId() != null) {
                IMap<String, Object> map = client.getMap(tapDropTableEvent.getTableId());
                map.destroy();
                IList<String> list = client.getList(tapDropTableEvent.getTableId());
                list.remove(tapDropTableEvent.getTableId());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ", e);
        }
    }

    private void clearIMap(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        try {
            if (tapClearTableEvent.getTableId() != null) {
                IMap<String, Object> map = client.getMap(tapClearTableEvent.getTableId());
                map.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Clear Table " + tapClearTableEvent.getTableId() + " Failed! \n ", e);
        }
    }


    private CreateTableOptions createIMap(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {

        try {
            if (tapCreateTableEvent.getTableId() != null) {
                IList<String> discoverSchemaIMaps = client.getList(TAPDATA_TABLE_LIST);
                String tableId = tapCreateTableEvent.getTableId();
                if (!discoverSchemaIMaps.contains(tableId))
                    discoverSchemaIMaps.add(tableId);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapCreateTableEvent.getTableId() + " Failed! \n ");
        }
        return null;
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        DataMap connectionConfig = connectionContext.getConnectionConfig();
        if (Objects.isNull(connectionConfig)) {
            throw new CoreException("connectionConfig cannot be null");
        }
        HazelcastInstance client = null;
        try {
            client = HazelcastClientUtil.getClient(connectionContext);
            IMap<String, String> tapDataConnectionTest = null;
            tapDataConnectionTest = client.getMap("tapDataConnectionTest");
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY,
                    "Connecting to the cluster succeeded."));
            tapDataConnectionTest.put("1", "2");
            tapDataConnectionTest.get("1");
            tapDataConnectionTest.put("1", "3");
            tapDataConnectionTest.clear();
            tapDataConnectionTest.destroy();
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, "Create,Insert,Update,Delete,Drop succeed"));
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED,
                    "Check whether the token,password,cluster ID or SSL protocol file is correct: " + e.getMessage()));
        } finally {
            if (client != null)
                HazelcastClientUtil.closeClient(client);
        }
        return null;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        try {
            IList<String> discoverSchemaIMaps = client.getList(TAPDATA_TABLE_LIST);
            return discoverSchemaIMaps.size();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Description Failed to obtain the number of iMaps.", e);
        }
    }
}
