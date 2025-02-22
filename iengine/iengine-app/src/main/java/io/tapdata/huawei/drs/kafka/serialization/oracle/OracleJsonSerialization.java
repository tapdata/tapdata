package io.tapdata.huawei.drs.kafka.serialization.oracle;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.huawei.drs.kafka.OracleOpType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.huawei.drs.kafka.serialization.IType;
import io.tapdata.huawei.drs.kafka.serialization.JsonSerialization;
import io.tapdata.huawei.drs.kafka.serialization.oracle.types.BinaryOracleType;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/22 16:25 Create
 */
@Slf4j
public class OracleJsonSerialization extends JsonSerialization {

    public static String KEY_OP = "opType";
    public static String KEY_FIELD_TYPE = "columnType";
    public static String KEY_AFTER = "data";
    public static String KEY_BEFORE = "old";
    private static final Map<String, IType> TYPE_MAP = new HashMap<String, IType>() {{
        new BinaryOracleType().append2(this);
    }};

    @Override
    public IType getType(String type) {
        return TYPE_MAP.get(type);
    }

    @Override
    public void process(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer, HazelcastProcessorBaseNode.ProcessResult processResult) {
        TapEvent tapEvent = tapdataEvent.getTapEvent();
        String tableName = TapEventUtil.getTableId(tapEvent);
        TapInsertRecordEvent recordEvent = (TapInsertRecordEvent) tapEvent;
        Map<String, Object> kafkaValues = TapEventUtil.getAfter(tapEvent);
        if (null == kafkaValues) return; // 没有数据，忽略

        Object kafkaValue = kafkaValues.get("value");
        if (!(kafkaValue instanceof byte[])) {
            throw new RuntimeException("value is not json");
        }

        JSONObject jsonValue;
        try {
            jsonValue = JSONObject.parseObject(new String((byte[]) kafkaValue));
        } catch (Exception e) {
            throw new RuntimeException("Illegal data: " + tapdataEvent);
        }

        String opStr = jsonValue.getString(KEY_OP);
        OracleOpType opType = OracleOpType.fromValue(opStr);
        switch (opType) {
            case UPDATE: {
                JSONObject fieldTypes = jsonValue.getJSONObject(KEY_FIELD_TYPE);
                JSONArray afterArr = jsonValue.getJSONArray(KEY_AFTER);
                JSONArray beforeArr = jsonValue.getJSONArray(KEY_BEFORE);
                TapdataEvent sendEvent = (TapdataEvent) tapdataEvent.clone();
                for (int i = 0; i < afterArr.size(); i++) {
                    JSONObject afterJson = afterArr.getJSONObject(i);
                    JSONObject beforeJson = beforeArr.getJSONObject(i);
                    sendEvent.setTapEvent(TapUpdateRecordEvent.create()
                        .table(tableName)
                        .referenceTime(recordEvent.getReferenceTime())
                        .after(encode(fieldTypes, afterJson))
                        .before(encode(fieldTypes, beforeJson))
                    );
                    consumer.accept(sendEvent, processResult);
                }
                break;
            }
            default:
                throw new RuntimeException("un-support event type '" + opStr + "', skip it: " + tapdataEvent.getOffset());
        }
    }

}
