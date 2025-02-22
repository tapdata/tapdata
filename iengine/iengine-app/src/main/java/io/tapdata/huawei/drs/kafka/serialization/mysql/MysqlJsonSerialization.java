package io.tapdata.huawei.drs.kafka.serialization.mysql;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.huawei.drs.kafka.MysqlOpType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.huawei.drs.kafka.serialization.IType;
import io.tapdata.huawei.drs.kafka.serialization.JsonSerialization;
import io.tapdata.huawei.drs.kafka.serialization.mysql.types.*;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * JSON 序列化实现
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/18 16:41 Create
 */
@Slf4j
public class MysqlJsonSerialization extends JsonSerialization {
    public static String KEY_OP = "type";
    public static String KEY_FIELD_TYPE = "mysqlType";
    public static String KEY_AFTER = "data";
    public static String KEY_BEFORE = "old";
    private static final Map<String, IType> TYPE_MAP = new HashMap<String, IType>() {{
        new BigintMysqlType().append2(this);
        new BinaryMysqlType().append2(this);
        new BlobMysqlType().append2(this);
        new CharMysqlType().append2(this);
        new DateMysqlType().append2(this);
        new DatetimeMysqlType().append2(this);
        new DecimalMysqlType().append2(this);
        new DoubleMysqlType().append2(this);
        new FloatMysqlType().append2(this);
        new IntMysqlType().append2(this);
        new TimeMysqlType().append2(this);
        new TextMysqlType().append2(this);
        new TimestampMysqlType().append2(this);
        new VarbinaryMysqlType().append2(this);
        new VarcharMysqlType().append2(this);
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
        MysqlOpType opType = MysqlOpType.fromValue(opStr);
        switch (opType) {
            case INSERT:
            case INIT: {
                JSONObject fieldTypes = jsonValue.getJSONObject(KEY_FIELD_TYPE);
                JSONArray afterArr = jsonValue.getJSONArray(KEY_AFTER);
                TapdataEvent sendEvent = (TapdataEvent) tapdataEvent.clone();
                for (int i = 0; i < afterArr.size(); i++) {
                    JSONObject afterJson = afterArr.getJSONObject(i);
                    sendEvent.setTapEvent(TapInsertRecordEvent.create()
                        .table(tableName)
                        .referenceTime(recordEvent.getReferenceTime())
                        .after(encode(fieldTypes, afterJson))
                    );
                    consumer.accept(sendEvent, processResult);
                }
                break;
            }
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
            case DELETE: {
                JSONObject fieldTypes = jsonValue.getJSONObject(KEY_FIELD_TYPE);
                JSONArray beforeArr = jsonValue.getJSONArray(KEY_BEFORE);
                TapdataEvent sendEvent = (TapdataEvent) tapdataEvent.clone();
                for (int i = 0; i < beforeArr.size(); i++) {
                    JSONObject beforeJson = beforeArr.getJSONObject(i);
                    sendEvent.setTapEvent(TapDeleteRecordEvent.create()
                        .table(tableName)
                        .referenceTime(recordEvent.getReferenceTime())
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
