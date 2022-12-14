package io.tapdata.connector.redis.writer;

import io.tapdata.connector.redis.RedisContext;
import io.tapdata.connector.redis.constant.ValueDataEnum;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import redis.clients.jedis.Pipeline;

import java.util.Map;

public class StringRedisRecordWriter extends AbstractRedisRecordWriter {

    public StringRedisRecordWriter(RedisContext redisContext, TapTable tapTable) {
        super(redisContext, tapTable);
    }

    @Override
    protected void handleInsertEvent(TapInsertRecordEvent event, Pipeline pipelined) {
        Map<String, Object> value = event.getAfter();
        if (ValueDataEnum.JSON.getType().equals(redisConfig.getValueData())) {
            writeJsonData(value, event, pipelined);
        } else {
            writeTextData(value, event, pipelined);
        }
    }

    @Override
    protected void handleUpdateEvent(TapUpdateRecordEvent event, Pipeline pipelined) {
        Map<String, Object> value = event.getAfter();
        if (ValueDataEnum.JSON.getType().equals(redisConfig.getValueData())) {
            writeJsonData(value, event, pipelined);
        } else {
            writeTextData(value, event, pipelined);
        }
    }

    @Override
    protected void handleDeleteEvent(TapDeleteRecordEvent event, Pipeline pipelined) {
        Map<String, Object> value = event.getBefore();
        if (ValueDataEnum.JSON.getType().equals(redisConfig.getValueData())) {
            writeJsonData(value, event, pipelined);
        } else {
            writeTextData(value, event, pipelined);
        }
    }

    private void writeJsonData(Map<String, Object> value, TapRecordEvent recordEvent, Pipeline pipelined) {
        if (EmptyKit.isEmpty(value)) {
            return;
        }
        String key = getRedisKey(value);
        if (recordEvent instanceof TapDeleteRecordEvent) {
            pipelined.del(key);
        } else {
            String strValue = getJsonValue(value);
            pipelined.set(key, strValue);
        }
    }

    private void writeTextData(Map<String, Object> value, TapRecordEvent recordEvent, Pipeline pipelined) {
        if (EmptyKit.isEmpty(value)) {
            return;
        }
        String key = getRedisKey(value);
        if (recordEvent instanceof TapDeleteRecordEvent) {
            pipelined.del(key);
        } else {
            String strValue = getTextValue(value);
            pipelined.set(key, strValue);
        }
    }

}
