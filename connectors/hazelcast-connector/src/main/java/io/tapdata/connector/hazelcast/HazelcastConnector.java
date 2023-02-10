package io.tapdata.connector.hazelcast;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.hazelcast.util.HazelcastClientUtil;
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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Author:Skeet
 * Date: 2023/2/7
 **/
@TapConnectorClass("spec_hazelcast.json")
public class HazelcastConnector extends ConnectorBase {
    public static final String TAG = HazelcastConnector.class.getSimpleName();
    private static final String TAPDATA_TABLE_LIST = "__tapdata__discoverSchemaIMaps";
    private HazelcastInstance client;


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
        HazelcastClientUtil.closeClient(client);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createIMap);
        connectorFunctions.supportClearTable(this::clearIMap);
        connectorFunctions.supportDropTable(this::dropIMap);
        connectorFunctions.supportGetTableNamesFunction(this::getImapNames);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        try {
            IList<String> iMaps = client.getList(TAPDATA_TABLE_LIST);
            List<TapTable> tapTableList = TapSimplify.list();
            if (iMaps.size() > 0) {
                for (String s : iMaps) {
                    //TODO 参考tables == null 或者empty， 全返回， 否则只返回tables指定的
                    tapTableList.add(table(s));
                }
            }
            consumer.accept(tapTableList);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("DiscoverSchema failure: " + e.getMessage(), e);
        } finally {
            client.shutdown();
        }
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> events, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        TapLogger.info(TAG, "batch events length is: {}", events.size());
        IMap<Object, Object> map = client.getMap(table.getId());
        if (events instanceof TapInsertRecordEvent) {
            final TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) events;
            final Map<String, Object> after = insertRecordEvent.getAfter();
            Map<String, Object> keyFromData = ObjectKey.getKeyFromData(null, after, table.primaryKeys(true));
            map.put(keyFromData, after);
        } else if (events instanceof TapUpdateRecordEvent) {
            final TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) events;
            final Map<String, Object> before = updateRecordEvent.getBefore();
            final Map<String, Object> after = updateRecordEvent.getAfter();
            Map<String, Object> keyFromData = ObjectKey.getKeyFromData(before, after, table.primaryKeys(true));
            Map<String, Object> oldValue = (Map<String, Object>) map.get(keyFromData);
            if (oldValue != null) {
                oldValue.putAll(after);
                map.put(keyFromData, oldValue);
            } else {
                map.put(keyFromData, after);
            }
        } else {
            final TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) events;
            final Map<String, Object> before = deleteRecordEvent.getBefore();
            Map<String, Object> keyFromData = ObjectKey.getKeyFromData(before, null, table.primaryKeys(true));
            map.delete(keyFromData);
        }
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
                IMap<Object, Object> map = client.getMap(tapDropTableEvent.getTableId());
                map.destroy();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ", e);
        }
    }

    private void clearIMap(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        try {
            if (tapClearTableEvent.getTableId() != null) {
                IMap<Object, Object> map = client.getMap(tapClearTableEvent.getTableId());
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
                IList<Object> discoverSchemaIMaps = client.getList(TAPDATA_TABLE_LIST);
                String tableId = tapCreateTableEvent.getTableId();
                if (discoverSchemaIMaps.contains(tableId))
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

        IMap<String, String> tapDataConnectionTest = null;
        try {
            HazelcastInstance client = HazelcastClientUtil.getClient(connectionContext);
            tapDataConnectionTest = client.getMap("tapDataConnectionTest");
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY,
                    "Connecting to the cluster succeeded."));
        } catch (Exception e) {
            e.printStackTrace();
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage()));
        }

        try {
            tapDataConnectionTest.put("1", "2");
            tapDataConnectionTest.get("1");
            tapDataConnectionTest.put("1", "3");
            tapDataConnectionTest.clear();
            tapDataConnectionTest.destroy();
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, "Create,Insert,Update,Delete,Drop succeed"));
        } catch (Exception e) {
            e.printStackTrace();
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, e.getMessage()));
        }
        return null;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        try {
            IList<Object> discoverSchemaIMaps = client.getList(TAPDATA_TABLE_LIST);
            return discoverSchemaIMaps.size();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Description Failed to obtain the number of iMaps.", e);
        }
    }
}
