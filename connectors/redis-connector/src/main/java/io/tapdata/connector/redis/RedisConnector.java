package io.tapdata.connector.redis;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.commons.collections.CollectionUtils;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author lemon
 */
@TapConnectorClass("spec_redis.json")
public class RedisConnector extends ConnectorBase {

    private RedisConfig redisConfig;

    private RedisContext redisContext;

    private  final  static String INIT_TABLE_NAME="tapdata";



    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {

       this.redisConfig = new RedisConfig().load(connectionContext.getConnectionConfig());
       this.redisContext = new RedisContext(redisConfig);

    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        redisContext.close();
    }

    /**
     * Register connector capabilities here.
     * <p>
     * To be as a source, please implement at least one of batchReadFunction or streamReadFunction.
     * To be as a target, please implement WriteRecordFunction.
     * To be as a source and target, please implement the functions that source and target required.
     *
     * @param connectorFunctions
     * @param codecRegistry
     */
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);

    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        Jedis jedis = redisContext.getJedis();
        Set<String> cacheTables;
        List<TapTable> tapTableList = list();
        TapTable tapTable  = new TapTable();
        tapTable.setName(INIT_TABLE_NAME);
        tapTableList.add(tapTable);
        cacheTables = jedis.smembers(RedisRecordWriter.JSON_REDIS_TABLES);
        if(!CollectionUtils.isEmpty(cacheTables)){
            for (String tableName:cacheTables){
                TapTable tapTableTemp  = new TapTable();
                tapTableTemp.setName(tableName);
                tapTableList.add(tapTableTemp);
            }

        }
        consumer.accept(tapTableList);

    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        redisConfig = new RedisConfig().load(connectionContext.getConnectionConfig());
        RedisTest redisTest = new RedisTest(redisConfig);
        TestItem testHostPort = redisTest.testHostPort();
        consumer.accept(testHostPort);
        if (testHostPort.getResult() == TestItem.RESULT_FAILED) {
            redisTest.close();
            return null;
        }
        TestItem testConnect = redisTest.testConnect();
        consumer.accept(testConnect);
        redisTest.close();
        return null;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        Jedis jedis = redisContext.getJedis();
        Set<String> cacheTables = jedis.smembers(RedisRecordWriter.JSON_REDIS_TABLES);
        if(!CollectionUtils.isEmpty(cacheTables)){
            return cacheTables.size()+1;
        }
        return 1;
    }


    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        new RedisRecordWriter(redisContext, tapTable, connectorContext).write(tapRecordEvents, writeListResultConsumer);
    }


    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws Throwable {

        DataMap nodeConfig = tapConnectorContext.getNodeConfig();
        String keyName = tapClearTableEvent.getTableId();
        if(nodeConfig !=null) {
            keyName = (String) nodeConfig.get("prefixKey");
        }
        Jedis jedis = redisContext.getJedis();
        jedis.del(keyName);
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws Throwable {
        DataMap nodeConfig = tapConnectorContext.getNodeConfig();
        String keyName = tapDropTableEvent.getTableId();
        if(nodeConfig !=null) {
           keyName = (String) nodeConfig.get("prefixKey");
        }
        Jedis jedis = redisContext.getJedis();
        jedis.del(keyName);
    }
}
