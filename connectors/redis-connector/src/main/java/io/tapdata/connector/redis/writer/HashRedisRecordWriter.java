package io.tapdata.connector.redis.writer;

import io.tapdata.connector.redis.RedisContext;
import io.tapdata.connector.redis.RedisPipeline;
import io.tapdata.connector.redis.constant.ValueDataEnum;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;

import java.util.HashMap;
import java.util.Map;

public class HashRedisRecordWriter extends AbstractRedisRecordWriter {

    private final String keyName;

    public HashRedisRecordWriter(RedisContext redisContext, TapTable tapTable) {
        super(redisContext, tapTable);
        keyName = tapTable.getId();
    }

    @Override
    protected void handleInsertEvent(TapInsertRecordEvent event, RedisPipeline pipelined) {
        Map<String, Object> value = event.getAfter();
        String fieldName = getRedisKey(value);
        if (redisConfig.getOneKey()) {
            String strValue = ValueDataEnum.JSON.getType().equals(redisConfig.getValueData()) ? getJsonValue(value) : getTextValue(value);
            pipelined.hset(keyName, fieldName, strValue);
        } else {
            pipelined.hmset(fieldName, toStringMap(value));
        }
    }

    @Override
    protected void handleUpdateEvent(TapUpdateRecordEvent event, RedisPipeline pipelined) {
        Map<String, Object> beforeValue = event.getBefore();
        Map<String, Object> afterValue = event.getAfter();
        Map<String, Object> lastBefore = new HashMap<>();
        if (EmptyKit.isNotEmpty(beforeValue)) {
            lastBefore.putAll(beforeValue);
        } else {
            keyFieldList.forEach(v -> lastBefore.put(v, afterValue.get(v)));
        }
        if (redisConfig.getOneKey()) {
            pipelined.hdel(keyName, getRedisKey(lastBefore));
            String strValue = ValueDataEnum.JSON.getType().equals(redisConfig.getValueData()) ? getJsonValue(afterValue) : getTextValue(afterValue);
            pipelined.hset(keyName, getRedisKey(afterValue), strValue);
        } else {
            pipelined.del(getRedisKey(lastBefore));
            pipelined.hmset(getRedisKey(afterValue), toStringMap(afterValue));
        }
    }

    @Override
    protected void handleDeleteEvent(TapDeleteRecordEvent event, RedisPipeline pipelined) {
        Map<String, Object> value = event.getBefore();
        String fieldName = getRedisKey(value);
        if (redisConfig.getOneKey()) {
            String strValue = ValueDataEnum.JSON.getType().equals(redisConfig.getValueData()) ? getJsonValue(value) : getTextValue(value);
            pipelined.hdel(keyName, fieldName, strValue);
        } else {
            pipelined.del(fieldName);
        }
    }

    private Map<String, String> toStringMap(Map<String, Object> map) {
        Map<String, String> newMap = new HashMap<>();
        map.forEach((k, v) -> newMap.put(k, EmptyKit.isNull(v) ? "null" : String.valueOf(v)));
        return newMap;
    }

}
