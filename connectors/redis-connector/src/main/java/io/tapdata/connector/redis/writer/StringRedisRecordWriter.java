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

public class StringRedisRecordWriter extends AbstractRedisRecordWriter {

    public StringRedisRecordWriter(RedisContext redisContext, TapTable tapTable) {
        super(redisContext, tapTable);
    }

    @Override
    protected void handleInsertEvent(TapInsertRecordEvent event, RedisPipeline pipelined) {
        Map<String, Object> value = event.getAfter();
        String strValue = ValueDataEnum.JSON.getType().equals(redisConfig.getValueData()) ? getJsonValue(value) : getTextValue(value);
        pipelined.set(getRedisKey(value), strValue);
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
        pipelined.del(getRedisKey(lastBefore));
        String strValue = ValueDataEnum.JSON.getType().equals(redisConfig.getValueData()) ? getJsonValue(afterValue) : getTextValue(afterValue);
        pipelined.set(getRedisKey(afterValue), strValue);
    }

    @Override
    protected void handleDeleteEvent(TapDeleteRecordEvent event, RedisPipeline pipelined) {
        pipelined.del(getRedisKey(event.getBefore()));
    }

}
