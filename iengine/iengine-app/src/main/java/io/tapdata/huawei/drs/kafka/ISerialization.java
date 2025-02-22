package io.tapdata.huawei.drs.kafka;

import com.alibaba.fastjson.JSONObject;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.huawei.drs.kafka.StoreType;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.huawei.drs.kafka.serialization.IType;
import io.tapdata.huawei.drs.kafka.serialization.JsonSerialization;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 17:24 Create
 */
public interface ISerialization {
    IType getType(String type);

    void process(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer, HazelcastProcessorBaseNode.ProcessResult processResult);

    default Map<String, Object> encode(JSONObject jsonType, JSONObject jsonData) {
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

    static ISerialization create(String storeType, String fromDBType) {
        StoreType storeTypeEnum = StoreType.fromValue(storeType);
        switch (storeTypeEnum) {
            case JSON:
                return JsonSerialization.create(fromDBType);
            case AVRO:
            case JSON_C:
            default:
                throw new RuntimeException("un-support storeType '" + storeType + "'");
        }
    }
}
