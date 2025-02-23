package io.tapdata.huawei.drs.kafka.serialization;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.huawei.drs.kafka.FromDBType;
import com.tapdata.tm.utils.TimeMaxAccepter;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.huawei.drs.kafka.ISerialization;
import io.tapdata.huawei.drs.kafka.IType;
import io.tapdata.huawei.drs.kafka.serialization.mysql.MysqlJsonSerialization;
import io.tapdata.huawei.drs.kafka.serialization.oracle.OracleJsonSerialization;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * JSON 序列化实现
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 16:41 Create
 */
@Slf4j
public abstract class JsonSerialization implements ISerialization {

    private final Map<String, IType> TYPES = new HashMap<>();
    private final TimeMaxAccepter timeMaxAccepter = new TimeMaxAccepter(10, 1);

    public JsonSerialization(IType... types) {
        for (IType type : types) {
            type.append2(TYPES);
        }
    }

    @Override
    public IType getType(String type) {
        return TYPES.get(type);
    }

    @Override
    public void process(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer, HazelcastProcessorBaseNode.ProcessResult processResult) {
        TapEvent tapEvent = tapdataEvent.getTapEvent();
        String tableName = TapEventUtil.getTableId(tapEvent);
        TapInsertRecordEvent recordEvent = (TapInsertRecordEvent) tapEvent;
        Map<String, Object> kafkaValues = TapEventUtil.getAfter(tapEvent);
        if (null == kafkaValues) return; // 没有数据，忽略

        Consumer<TapEvent> sender = event -> {
            TapdataEvent sendEvent = (TapdataEvent) tapdataEvent.clone();
            sendEvent.setTapEvent(event);
            consumer.accept(sendEvent, processResult);
        };

        Long referenceTime = recordEvent.getReferenceTime();
        JSONObject jsonValue = getValue(kafkaValues);
        String opStr = getOp(jsonValue);
        if (!decodeRecord(tableName, referenceTime, jsonValue, opStr, sender)) {
            if (timeMaxAccepter.check()) {
                log.warn("un-support event type '{}', skip it: {}({}-{})"
                    , opStr, tableName, kafkaValues.get("partition"), kafkaValues.get("offset"));
            }
        }
    }

    protected JSONObject getValue(Map<String, Object> kafkaValues) {
        Object kafkaValue = kafkaValues.get("value");
        try {
            if (!(kafkaValue instanceof byte[])) {
                kafkaValue = JSONObject.toJSONBytes(kafkaValue);
            }
            return JSONObject.parseObject(new String((byte[]) kafkaValue));
        } catch (Exception e) {
            throw new RuntimeException("Illegal data: " + JSONObject.toJSONString(kafkaValues));
        }
    }

    protected abstract String getOp(JSONObject jsonValue);

    protected abstract JSONObject getFieldTypes(JSONObject jsonValue);

    protected abstract JSONArray getAfter(JSONObject jsonValue);

    protected abstract JSONArray getBefore(JSONObject jsonValue);

    protected abstract boolean decodeRecord(String tableName, Long referenceTime, JSONObject jsonValue, String opStr, Consumer<TapEvent> sender);

    protected Map<String, Object> decodeValue(JSONObject jsonType, JSONObject jsonData) {
        Map<String, Object> result = new LinkedHashMap<>();

        String fieldType;
        Object fieldValue;
        IType typeSerialization;
        for (String field : jsonType.keySet()) {
            fieldType = jsonType.getString(field);
            if (!jsonData.containsKey(field)) continue;
            fieldValue = jsonData.get(field);
            if (null != fieldValue) {
                typeSerialization = getType(fieldType);
                if (null != typeSerialization) {
                    fieldValue = typeSerialization.decode(fieldValue);
                }
            }
            result.put(field, fieldValue);
        }
        return result;
    }

    protected boolean decodeInsertRecord(String tableName, Long referenceTime, JSONObject jsonValue, Consumer<TapEvent> consumer) {
        JSONObject fieldTypes = getFieldTypes(jsonValue);
        JSONArray afterArr = getAfter(jsonValue);
        for (int i = 0; i < afterArr.size(); i++) {
            JSONObject afterJson = afterArr.getJSONObject(i);
            consumer.accept(TapInsertRecordEvent.create()
                .table(tableName)
                .referenceTime(referenceTime)
                .after(decodeValue(fieldTypes, afterJson))
            );
        }
        return true;
    }

    protected boolean decodeUpdateRecord(String tableName, Long referenceTime, JSONObject jsonValue, Consumer<TapEvent> consumer) {
        JSONObject fieldTypes = getFieldTypes(jsonValue);
        JSONArray afterArr = getAfter(jsonValue);
        JSONArray beforeArr = getBefore(jsonValue);
        for (int i = 0; i < afterArr.size(); i++) {
            JSONObject afterJson = afterArr.getJSONObject(i);
            JSONObject beforeJson = beforeArr.getJSONObject(i);
            consumer.accept(TapUpdateRecordEvent.create()
                .table(tableName)
                .referenceTime(referenceTime)
                .after(decodeValue(fieldTypes, afterJson))
                .before(decodeValue(fieldTypes, beforeJson))
            );
        }
        return true;
    }

    protected boolean decodeDeleteRecord(String tableName, Long referenceTime, JSONObject jsonValue, Consumer<TapEvent> consumer) {
        JSONObject fieldTypes = getFieldTypes(jsonValue);
        JSONArray beforeArr = getBefore(jsonValue);
        for (int i = 0; i < beforeArr.size(); i++) {
            JSONObject beforeJson = beforeArr.getJSONObject(i);
            consumer.accept(TapDeleteRecordEvent.create()
                .table(tableName)
                .referenceTime(referenceTime)
                .before(decodeValue(fieldTypes, beforeJson))
            );
        }
        return true;
    }

    public static JsonSerialization create(String fromDBType) {
        FromDBType fromDBTypeEnum = FromDBType.fromValue(fromDBType);
        switch (fromDBTypeEnum) {
            case MYSQL:
            case GAUSSDB_MYSQL:
                return new MysqlJsonSerialization();
            case ORACLE:
            case MSSQL:
            case POSTGRESQL:
            case GAUSSDB:
                return new OracleJsonSerialization();
            default:
                throw new RuntimeException("un-support DB type '" + fromDBType + "'");
        }
    }
}
