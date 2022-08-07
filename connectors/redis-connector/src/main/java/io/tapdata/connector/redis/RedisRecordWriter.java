package io.tapdata.connector.redis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections.MapUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author lemon
 */
public class RedisRecordWriter {


    private final Jedis jedis;

    private final RedisContext redisContext;

    private final static int BATCH_SIZE = 100;

    public final static String VALUE_TYPE_LIST = "list";

    public final static String JSON_REDIS_TABLES = "tapdataJson";

    private final TapConnectorContext connectorContext;


    public RedisRecordWriter(RedisContext redisContext, TapTable tapTable, TapConnectorContext connectorContext) {
        this.redisContext = redisContext;
        this.jedis = redisContext.getJedis();
        this.connectorContext = connectorContext;
    }



    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {

        DataMap nodeConfig = connectorContext.getNodeConfig();
        String valueType;
        if (nodeConfig == null) {
            valueType = VALUE_TYPE_LIST;
        } else {
            valueType = (String) nodeConfig.get("valueType");
        }

        if (VALUE_TYPE_LIST.equals(valueType)) {
            handleListData(tapRecordEvents, writeListResultConsumer);
        } else {

            handleJsonData(tapRecordEvents, writeListResultConsumer);
        }
    }


    /**
     * 处理json格式的数据
     */
    private void handleJsonData(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        //result of these events
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        long insert = 0L;
        long update = 0L;
        long delete = 0L;
        Pipeline pipelined = jedis.pipelined();
        try {
            int handleCount = 0;
            for (TapRecordEvent recordEvent : tapRecordEvents) {
                handleCount++;
                if (recordEvent instanceof TapInsertRecordEvent) {
                    TapInsertRecordEvent tapInsertRecordEvent = (TapInsertRecordEvent) recordEvent;
                    Map<String, Object> value = tapInsertRecordEvent.getAfter();
                    String tableName = tapInsertRecordEvent.getTableId();
                    writeData(value, tapInsertRecordEvent, pipelined, tableName);
                    insert++;
                } else if (recordEvent instanceof TapUpdateRecordEvent) {
                    TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                    Map<String, Object> value = tapUpdateRecordEvent.getAfter();
                    String tableName = tapUpdateRecordEvent.getTableId();
                    writeData(value, tapUpdateRecordEvent, pipelined, tableName);
                    update++;

                } else {
                    TapDeleteRecordEvent tapDeleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                    Map<String, Object> value = tapDeleteRecordEvent.getBefore();
                    String tableName = tapDeleteRecordEvent.getTableId();
                    writeData(value, tapDeleteRecordEvent, pipelined, tableName);
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
            jedis.close();
        }
    }

    /**
     * 组装pipeline数据
     */
    private void writeData(Map<String, Object> value, TapRecordEvent recordEvent, Pipeline pipelined, String tableName) {
        if (MapUtils.isEmpty(value)) {
            TapLogger.warn("Message data is empty {} will skip it.", JSON.toJSONString(recordEvent));
            return;
        }
        String key = redisContext.getRedisKeySetter().getRedisKey(value, connectorContext, tableName);
        // json格式平铺
        String strValue = getStrValue(value);
        if (recordEvent instanceof TapDeleteRecordEvent) {
            pipelined.del(key);
        } else {
            pipelined.set(key, strValue);
        }

        boolean flag = redisContext.getRedisKeySetter().tableIsExist(tableName);
        if (flag) {
            pipelined.sadd(JSON_REDIS_TABLES, tableName);
        }
    }


    /**
     * handle data for string
     *
     * @return String
     */
    private String getStrValue(Map<String, Object> map) {
        JSONObject jsonObject = new JSONObject();
        for (String key : map.keySet()) {
            jsonObject.put(key, map.get(key));
        }

        return JSONObject.toJSONString(jsonObject);
    }


    /**
     * 处理list格式的数据
     */
    private void handleListData(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {

        //result of these events
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        long insert = 0L;
        long update = 0L;
        long delete = 0L;
        int handleCount = 0;
        Pipeline pipelined = jedis.pipelined();
        try {
            for (TapRecordEvent recordEvent : tapRecordEvents) {
                handleCount++;
                if (recordEvent instanceof TapInsertRecordEvent) {
                    TapInsertRecordEvent tapInsertRecordEvent = (TapInsertRecordEvent) recordEvent;
                    Map<String, Object> value = tapInsertRecordEvent.getAfter();
                    String tableName = tapInsertRecordEvent.getTableId();
                    String keyName = redisContext.getRedisKeySetter().getRedisKey(value, connectorContext, tableName);
                    String strValue = getStrValue(value);
                    pipelined.lpush(keyName, strValue);
                    insert++;
                } else {
                    String tableName = recordEvent.getTableId();
                    List<String> list = new ArrayList<>(50000);
                    list = (List<String>) pipelined.lrange(tableName, 0, -1);
                    String value = "[";
                    for (String str : list) {
                        value = str + ",";
                    }
                    value = value.substring(0, value.length() - 1) + "]";
                    pipelined.set(tableName, value);
                    pipelined.rename(tableName, tableName);

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
            jedis.close();
        }
    }


}
