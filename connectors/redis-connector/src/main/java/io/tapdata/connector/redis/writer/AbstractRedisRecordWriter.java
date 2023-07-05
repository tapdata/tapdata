package io.tapdata.connector.redis.writer;

import io.tapdata.connector.redis.CommonJedis;
import io.tapdata.connector.redis.RedisConfig;
import io.tapdata.connector.redis.RedisContext;
import io.tapdata.connector.redis.RedisPipeline;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public abstract class AbstractRedisRecordWriter {

    protected final CommonJedis jedis;
    protected final RedisConfig redisConfig;
    protected final TapTable tapTable;
    protected final List<String> fieldList;
    protected final List<String> keyFieldList;
    protected static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class); //json util

    public AbstractRedisRecordWriter(RedisContext redisContext, TapTable tapTable) {
        this.redisConfig = redisContext.getRedisConfig();
        this.jedis = redisContext.getJedis();
        this.tapTable = tapTable;
        this.fieldList = tapTable.getNameFieldMap().entrySet().stream().sorted(Comparator.comparing(v ->
                EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).map(Map.Entry::getKey).collect(Collectors.toList());
        this.keyFieldList = getKeyFieldList();
    }

    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Exception {
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        long insert = 0L;
        long update = 0L;
        long delete = 0L;
        try (
                RedisPipeline pipelined = jedis.pipelined(redisConfig)
        ) {
            for (TapRecordEvent recordEvent : tapRecordEvents) {
                if (recordEvent instanceof TapInsertRecordEvent) {
                    TapInsertRecordEvent tapInsertRecordEvent = (TapInsertRecordEvent) recordEvent;
                    if (EmptyKit.isNull(tapInsertRecordEvent.getAfter())) {
                        continue;
                    }
                    handleInsertEvent(tapInsertRecordEvent, pipelined);
                    insert++;
                } else if (recordEvent instanceof TapUpdateRecordEvent) {
                    TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                    if (EmptyKit.isNull(tapUpdateRecordEvent.getAfter())) {
                        continue;
                    }
                    handleUpdateEvent(tapUpdateRecordEvent, pipelined);
                    update++;
                } else {
                    TapDeleteRecordEvent tapDeleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                    if (EmptyKit.isNull(tapDeleteRecordEvent.getBefore())) {
                        continue;
                    }
                    handleDeleteEvent(tapDeleteRecordEvent, pipelined);
                    delete++;
                }
            }
            pipelined.sync();
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

    protected abstract void handleInsertEvent(TapInsertRecordEvent event, RedisPipeline pipelined);

    protected abstract void handleUpdateEvent(TapUpdateRecordEvent event, RedisPipeline pipelined) throws Exception;

    protected abstract void handleDeleteEvent(TapDeleteRecordEvent event, RedisPipeline pipelined);

    protected String getRedisKey(Map<String, Object> value) {
        if (EmptyKit.isNotBlank(redisConfig.getKeyExpression())) {
            String key = redisConfig.getKeyExpression();
            for (String field : fieldList) {
                Object obj = value.get(field);
                key = key.replaceAll("\\$\\{" + field + "}", EmptyKit.isNull(obj) ? "null" : String.valueOf(obj));
            }
            return key;
        } else {
            StringBuilder sb = new StringBuilder();
            if (EmptyKit.isNotEmpty(redisConfig.getKeyPrefix())) {
                sb.append(redisConfig.getKeyPrefix());
            }
            sb.append(tapTable.primaryKeys(true).stream().map(k -> {
                Object obj = value.get(k);
                return EmptyKit.isNull(obj) ? "null" : String.valueOf(obj);
            }).collect(Collectors.joining(redisConfig.getKeyJoin())));
            if (EmptyKit.isNotEmpty(redisConfig.getKeySuffix())) {
                sb.append(redisConfig.getKeySuffix());
            }
            return sb.toString();
        }
    }

    protected List<String> getKeyFieldList() {
        List<String> keyFieldList = new ArrayList<>();
        String expression = redisConfig.getKeyExpression();
        if (EmptyKit.isBlank(expression)) {
            return keyFieldList;
        }
        for (String field : fieldList) {
            if (expression.contains("${" + field + "}")) {
                keyFieldList.add(field);
            }
        }
        return keyFieldList;
    }

    protected String getJsonValue(Map<String, Object> value) {
        return jsonParser.toJson(value.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> EmptyKit.isNull(v.getValue()) ? "null" : String.valueOf(v.getValue()))));
    }

    protected String getTextValue(Map<String, Object> value) {
        return fieldList.stream().map(v -> {
            Object obj = value.get(v);
            String str = EmptyKit.isNull(obj) ? "null" : String.valueOf(obj);
            if (redisConfig.getCsvFormat()) {
                return csvFormat(str, redisConfig.getValueJoinString());
            } else {
                return str;
            }
        }).collect(Collectors.joining(redisConfig.getValueJoinString()));
    }

    private String csvFormat(String str, String delimiter) {
        if (str.contains(delimiter)
                || str.contains("\t")
                || str.contains("\r")
                || str.contains("\n")
                || str.contains(" ")
                || str.contains("\"")) {
            return "\"" + str.replaceAll("\"", "\"\"") + "\"";
        }
        return str;
    }
}
