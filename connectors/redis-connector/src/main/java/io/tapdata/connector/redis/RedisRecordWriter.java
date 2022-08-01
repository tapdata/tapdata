package io.tapdata.connector.redis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.connector.constant.MapUtil;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections.MapUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author lemon
 */
public class RedisRecordWriter {


    private final Jedis jedis;
    private final TapTable tapTable;

    private final RedisContext redisContext;

    private final static  int BATCH_SIZE =100;

    public final static String VALUE_TYPE ="json";

    public final  static String HASH_REDIS_TABLES ="tapdataHash";

    public final  static String JSON_REDIS_TABLES ="tapdataJson";

    private TapConnectorContext connectorContext;



    public RedisRecordWriter(RedisContext redisContext, TapTable tapTable, TapConnectorContext connectorContext) {
        this.redisContext = redisContext;
        this.jedis = redisContext.getJedis();
        this.tapTable = tapTable;
        this.connectorContext = connectorContext;
    }


    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        //result of these events
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        long insert = 0L;
        long update = 0L;
        long delete = 0L;
        try (Pipeline pipelined = jedis.pipelined()) {
            int handleCount = 0;
            for (TapRecordEvent recordEvent : tapRecordEvents) {
                handleCount++;
                if (recordEvent instanceof TapInsertRecordEvent) {
                    TapInsertRecordEvent tapInsertRecordEvent = (TapInsertRecordEvent) recordEvent;
                    Map<String, Object> value = tapInsertRecordEvent.getAfter();
                    String tableName = tapInsertRecordEvent.getTableId();
                    writeData(value, tapInsertRecordEvent, pipelined,tableName);
                    insert++;
                } else if (recordEvent instanceof TapUpdateRecordEvent) {
                    TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                    Map<String, Object> value = tapUpdateRecordEvent.getAfter();
                    String tableName = tapUpdateRecordEvent.getTableId();
                    writeData(value, tapUpdateRecordEvent, pipelined,tableName);
                    update++;

                } else {
                    TapDeleteRecordEvent tapDeleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                    Map<String, Object> value = tapDeleteRecordEvent.getBefore();
                    String tableName = tapDeleteRecordEvent.getTableId();
                    writeData(value, tapDeleteRecordEvent, pipelined,tableName);
                    delete++;
                }

                if (handleCount >= BATCH_SIZE) {
                    pipelined.sync();
                    handleCount = 0;
                }
            }
            if (handleCount > 0) {
                pipelined.sync();
            }

            writeListResultConsumer.accept(listResult
                    .insertedCount(insert)
                    .modifiedCount(update)
                    .removedCount(delete));

        } catch (Exception e) {
            TapLogger.error("Redis write failed,message: {}", e.getMessage(), e);
            throw e;
        } finally {

            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 组装pipeline数据
     */
    private void writeData(Map<String, Object> value, TapRecordEvent recordEvent, Pipeline pipelined,String tableName) {
        if (MapUtils.isEmpty(value)) {
            TapLogger.warn("Message data is empty {} will skip it.", JSON.toJSONString(recordEvent));
            return;
        }
        String key = redisContext.getRedisKeySetter().getRedisKey(value, tapTable, connectorContext,tableName);
        if (recordEvent instanceof TapUpdateRecordEvent) {
            pipelined.del(key);
        } else {
            // json格式平铺 否则为hash模式
            if (VALUE_TYPE.equals(redisContext.getRedisConfig().getValueType())) {
                String strValue = getStrValue(value);
                pipelined.set(key, strValue);
            } else {
                Map<String, String> newMap = new HashMap<>(16);
                MapUtil.copyToNewMap(value, newMap);
                pipelined.hmset(key, newMap);
            }
        }

        boolean flag = redisContext.getRedisKeySetter().tableIsExist(tableName);
        if (flag) {
            if (VALUE_TYPE.equals(redisContext.getRedisConfig().getValueType())) {
                pipelined.sadd(JSON_REDIS_TABLES, tableName);
            } else {
                pipelined.sadd(HASH_REDIS_TABLES, tableName);
            }
        }
    }

    /**
     * handle data for string
     * @return String
     */
    private String getStrValue(Map<String, Object> map) {
        JSONObject jsonObject = new JSONObject();
        for (String key : map.keySet()) {
            jsonObject.put(key,map.get(key));
        }

        return JSONObject.toJSONString(jsonObject);
    }

}
