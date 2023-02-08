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
    private HazelcastInstance client;


    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        try {
            client = HazelcastClientUtil.getClient(connectionContext);
        } catch (Exception e) {
            e.printStackTrace();
            throw new CoreException("The Hazelcast cluster connection fails: " + e.getMessage());
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
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportErrorHandleFunction(this::handleErrors);

    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        try {
            IList<String> IMaps = client.getList("discoverSchemaIMaps");
            List<TapTable> tapTableList = TapSimplify.list();
            if (IMaps.size() > 0) {
                for (String s : IMaps) {
                    TapTable tapTable = table(s);
                    tapTableList.add(tapTable);
                }
            }
            consumer.accept(tapTableList);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("DiscoverSchema failure: " + e.getMessage());
        } finally {
            client.shutdown();
        }
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> events, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        TapLogger.info(TAG, "batch events length is: {}", events.size());
        IMap<Object, Object> map = client.getMap(table.getId());
        try {
            if (events instanceof TapInsertRecordEvent) {
                final TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) events;
                final Map<String, Object> after = insertRecordEvent.getAfter();
                Map<String, Object> keyFromData = ObjectKey.getKeyFromData(after, null, after.keySet());
                map.put(keyFromData.toString(), after);
            } else if (events instanceof TapUpdateRecordEvent) {
                final TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) events;
                final Map<String, Object> before = updateRecordEvent.getBefore();
                final Map<String, Object> after = updateRecordEvent.getAfter();
                Map<String, Object> keyFromData = ObjectKey.getKeyFromData(before, null, before.keySet());
                Map<String, Object> tempMap = map();
                map.put(keyFromData.toString(), before);
                tempMap.put(keyFromData.toString(), after);
                map.putAll(tempMap);
            } else {
                final TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) events;
                final Map<String, Object> before = deleteRecordEvent.getBefore();
                Map<String, Object> keyFromData = ObjectKey.getKeyFromData(before, null, before.keySet());
                map.delete(keyFromData.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void getImapNames(TapConnectionContext tapConnectionContext, int i, Consumer<List<String>> listConsumer) throws Exception {
        try {
            IList<String> discoverSchemaIMaps = client.getList("discoverSchemaIMaps");
            List<String> tableList = TapSimplify.list();
            if (discoverSchemaIMaps.size() > 0) {
                for (String name : discoverSchemaIMaps) {
                    tableList.add(name);
                }
            }
            if (!tableList.isEmpty()) {
                listConsumer.accept(tableList);
                tableList.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("getImapNames failed." + e.getMessage());
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
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
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
            throw new RuntimeException("Clear Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }


    private CreateTableOptions createIMap(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {

        try {
            if (tapCreateTableEvent.getTableId() != null) {
                client.getMap(tapCreateTableEvent.getTableId());
                IList<Object> discoverSchemaIMaps = client.getList("discoverSchemaIMaps");
                discoverSchemaIMaps.add(tapCreateTableEvent.getTableId());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapCreateTableEvent.getTableId() + " Failed! \n ");
        }
        return null;
    }

    private RetryOptions handleErrors(TapConnectionContext tapConnectionContext, PDKMethod pdkMethod, Throwable throwable) {
        return null;
    }

    private void queryByFilter(TapConnectorContext tapConnectorContext, List<TapFilter> tapFilters, TapTable table, Consumer<List<FilterResult>> listConsumer) {
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        DataMap connectionConfig = connectionContext.getConnectionConfig();
        if (Objects.isNull(connectionConfig)) {
            throw new CoreException("connectionConfig cannot be null");
        }

        IMap<String, String> tapDataConnectionTest = null;
        try {
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
            IList<Object> discoverSchemaIMaps = client.getList("discoverSchemaIMaps");
            return discoverSchemaIMaps.size();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Description Failed to obtain the number of iMaps.");
        }
    }
}
