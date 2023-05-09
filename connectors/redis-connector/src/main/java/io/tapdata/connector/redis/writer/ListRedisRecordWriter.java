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

public class ListRedisRecordWriter extends AbstractRedisRecordWriter {

    private String keyName;

    public ListRedisRecordWriter(RedisContext redisContext, TapTable tapTable) {
        super(redisContext, tapTable);
        keyName = tapTable.getId();
    }

    @Override
    protected void handleInsertEvent(TapInsertRecordEvent event, RedisPipeline pipelined) {
        Map<String, Object> value = event.getAfter();
        String strValue = ValueDataEnum.JSON.getType().equals(redisConfig.getValueData()) ? getJsonValue(value) : getTextValue(value);
        if (!redisConfig.getOneKey()) {
            keyName = getRedisKey(value);
        }
        pipelined.rpush(keyName, strValue);
    }

    @Override
    protected void handleUpdateEvent(TapUpdateRecordEvent event, RedisPipeline pipelined) throws Exception {
        Map<String, Object> afterValue = event.getAfter();
        String newValue = ValueDataEnum.JSON.getType().equals(redisConfig.getValueData()) ? getJsonValue(afterValue) : getTextValue(afterValue);
        if (null == event.getBefore()) {
            throw new Exception("Redis update failed  reason before data is null");
        }
        Map<String, Object> beforeValue = event.getBefore();
        Map<String, Object> lastBefore = new HashMap<>();
        if (EmptyKit.isNotEmpty(beforeValue)) {
            lastBefore.putAll(beforeValue);
        } else {
            keyFieldList.forEach(v -> lastBefore.put(v, afterValue.get(v)));
        }
        String oldValue = ValueDataEnum.JSON.getType().equals(redisConfig.getValueData()) ? getJsonValue(lastBefore) : getTextValue(lastBefore);
        if (!redisConfig.getOneKey()) {
            String newKeyName = getRedisKey(afterValue);
            String oldKeyName = getRedisKey(lastBefore);
            if (newKeyName.equals(oldKeyName)) {
                updateRedisList(pipelined, newKeyName, oldValue, newValue);
            } else {
                pipelined.lrem(oldKeyName, 1, oldValue);
                pipelined.rpush(newKeyName, newValue);
            }
        } else {
            updateRedisList(pipelined, keyName, oldValue, newValue);
        }
    }

    @Override
    protected void handleDeleteEvent(TapDeleteRecordEvent event, RedisPipeline pipelined) {
        Map<String, Object> value = event.getBefore();
        String oldValue = ValueDataEnum.JSON.getType().equals(redisConfig.getValueData()) ? getJsonValue(value) : getTextValue(value);
        if (redisConfig.getOneKey()) {
            pipelined.lrem(keyName, 1, oldValue);
        } else {
            pipelined.lrem(getRedisKey(value), 1, oldValue);
        }
    }

    private void updateRedisList(RedisPipeline pipelined, String keyName, String oldValue, String newValue) {
        pipelined.eval("local pos = redis.call('lpos', KEYS[1], ARGV[1]); if (not pos) then return end; redis.call('lset', KEYS[1], pos, ARGV[2]);", 1, keyName, oldValue, newValue);
    }

}
