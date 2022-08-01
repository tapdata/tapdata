package io.tapdata.connector.redis;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapNumberValue;
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


    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        Jedis jedis = redisContext.getJedis();
        Set<String> cacheTables;
        List<TapTable> tapTableList = list();
        TapTable tapTable  = new TapTable();
        tapTable.setName("tapdata");
        tapTableList.add(tapTable);
        if (RedisRecordWriter.VALUE_TYPE.equals(redisContext.getRedisConfig().getValueType())) {
            cacheTables = jedis.smembers(RedisRecordWriter.JSON_REDIS_TABLES);

        } else {
            cacheTables = jedis.smembers(RedisRecordWriter.HASH_REDIS_TABLES);
        }

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
        Set<String> cacheTables;
        if (RedisRecordWriter.VALUE_TYPE.equals(redisContext.getRedisConfig().getValueType())) {
            cacheTables = jedis.smembers(RedisRecordWriter.JSON_REDIS_TABLES);

        } else {
            cacheTables = jedis.smembers(RedisRecordWriter.HASH_REDIS_TABLES);
        }

        if(!CollectionUtils.isEmpty(cacheTables)){
            return cacheTables.size()+1;
        }
        return 1;
    }


    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        new RedisRecordWriter(redisContext, tapTable, connectorContext).write(tapRecordEvents, writeListResultConsumer);
    }
}
