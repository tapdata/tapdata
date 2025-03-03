package io.tapdata.huawei.drs.kafka.serialization;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.huawei.drs.kafka.FromDBType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.huawei.drs.kafka.serialization.oracle.OracleJsonSerialization;
import io.tapdata.huawei.drs.kafka.types.BytesType;
import io.tapdata.huawei.drs.kafka.types.StringType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/25 19:07 Create
 */
class JsonSerializationTest {

    static final String KEY_OP = "op";
    static final String KEY_FIELD_TYPES = "fieldTypes";
    static final String KEY_AFTER = "after";
    static final String KEY_BEFORE = "before";

    JsonSerialization ins;
    JsonSerialization mockIns;

    @BeforeEach
    void setUp() {
        ins = new MockJsonSerialization();
        mockIns = Mockito.spy(ins);
    }

    static class MockJsonSerialization extends JsonSerialization {
        public MockJsonSerialization() {
            super(false, new BytesType("bytes"), new StringType("string"));
        }

        @Override
        protected String getOp(JSONObject jsonValue) {
            return jsonValue.getString(KEY_OP);
        }

        @Override
        protected JSONObject getFieldTypes(JSONObject jsonValue) {
            return jsonValue.getJSONObject(KEY_FIELD_TYPES);
        }

        @Override
        protected JSONArray getAfter(JSONObject jsonValue) {
            return jsonValue.getJSONArray(KEY_AFTER);
        }

        @Override
        protected JSONArray getBefore(JSONObject jsonValue) {
            return jsonValue.getJSONArray(KEY_BEFORE);
        }

        @Override
        protected boolean decodeRecord(String tableName, Long referenceTime, JSONObject jsonValue, String opStr, Consumer<TapEvent> sender) {
            switch (opStr) {
                case "INSERT":
                    return decodeInsertRecord(tableName, referenceTime, jsonValue, sender);
                case "UPDATE":
                    return decodeUpdateRecord(tableName, referenceTime, jsonValue, sender);
                case "DELETE":
                    return decodeDeleteRecord(tableName, referenceTime, jsonValue, sender);
                default:
                    return false;
            }
        }
    }

    @Nested
    class CreateTest {
        @Test
        void testOracle() {
            JsonSerialization serialization = JsonSerialization.create(FromDBType.ORACLE.name(), false);
            Assertions.assertInstanceOf(OracleJsonSerialization.class, serialization);
        }

        @Test
        void testUnSupport() {
            Assertions.assertThrows(RuntimeException.class, () -> JsonSerialization.create("-", false));
        }
    }


    @Nested
    class ProcessTest {

        @Test
        void testUnSupportOp() {
            TapdataEvent tapdataEvent = new TapdataEvent();
            tapdataEvent.setTapEvent(TapInsertRecordEvent.create()
                    .after(Optional.of(new LinkedHashMap<String, Object>()).map(after -> {
                        LinkedHashMap<String, Object> insertRecord = new LinkedHashMap<>();
                        insertRecord.put(KEY_OP, "-");
                        after.put("value", insertRecord);
                        return after;
                    }).get())
            );

            BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = Mockito.mock(BiConsumer.class);
            HazelcastProcessorBaseNode.ProcessResult processResult = new HazelcastProcessorBaseNode.ProcessResult();

            mockIns.process(tapdataEvent, consumer, processResult);
            Mockito.verify(mockIns, Mockito.times(0)).decodeInsertRecord(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
            Mockito.verify(mockIns, Mockito.times(0)).decodeUpdateRecord(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
            Mockito.verify(mockIns, Mockito.times(0)).decodeDeleteRecord(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        }

        @Test
        void testInsertRecord() {
            TapdataEvent tapdataEvent = new TapdataEvent();
            tapdataEvent.setTapEvent(TapInsertRecordEvent.create()
                    .after(Optional.of(new LinkedHashMap<String, Object>()).map(after -> {
                        LinkedHashMap<String, Object> insertRecord = new LinkedHashMap<>();
                        insertRecord.put(KEY_OP, "INSERT");
                        insertRecord.put(KEY_FIELD_TYPES, Optional.of(new LinkedHashMap<>()).map(types -> {
                            types.put("id", "string");
                            return types;
                        }).get());
                        insertRecord.put(KEY_AFTER, new ArrayList<>(Collections.singletonList(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                            data.put("id", "1");
                            return data;
                        }).get())));
                        after.put("value", insertRecord);
                        return after;
                    }).get())
            );

            BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = Mockito.mock(BiConsumer.class);
            HazelcastProcessorBaseNode.ProcessResult processResult = new HazelcastProcessorBaseNode.ProcessResult();

            mockIns.process(tapdataEvent, consumer, processResult);
            Mockito.verify(mockIns, Mockito.times(1)).decodeInsertRecord(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        }

        @Test
        void testUpdateRecord() {
            TapdataEvent tapdataEvent = new TapdataEvent();
            tapdataEvent.setTapEvent(TapInsertRecordEvent.create()
                    .after(Optional.of(new LinkedHashMap<String, Object>()).map(after -> {
                        LinkedHashMap<String, Object> insertRecord = new LinkedHashMap<>();
                        insertRecord.put(KEY_OP, "UPDATE");
                        insertRecord.put(KEY_FIELD_TYPES, Optional.of(new LinkedHashMap<>()).map(types -> {
                            types.put("id", "string");
                            return types;
                        }).get());
                        insertRecord.put(KEY_AFTER, new ArrayList<>(Collections.singletonList(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                            data.put("id", "1");
                            return data;
                        }).get())));
                        insertRecord.put(KEY_BEFORE, new ArrayList<>(Collections.singletonList(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                            data.put("id", "1");
                            return data;
                        }).get())));
                        after.put("value", insertRecord);
                        return after;
                    }).get())
            );

            BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = Mockito.mock(BiConsumer.class);
            HazelcastProcessorBaseNode.ProcessResult processResult = new HazelcastProcessorBaseNode.ProcessResult();

            mockIns.process(tapdataEvent, consumer, processResult);
            Mockito.verify(mockIns, Mockito.times(1)).decodeUpdateRecord(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        }

        @Test
        void testDeleteRecord() {
            TapdataEvent tapdataEvent = new TapdataEvent();
            tapdataEvent.setTapEvent(TapInsertRecordEvent.create()
                    .after(Optional.of(new LinkedHashMap<String, Object>()).map(after -> {
                        LinkedHashMap<String, Object> insertRecord = new LinkedHashMap<>();
                        insertRecord.put(KEY_OP, "DELETE");
                        insertRecord.put(KEY_FIELD_TYPES, Optional.of(new LinkedHashMap<>()).map(types -> {
                            types.put("id", "string");
                            return types;
                        }).get());
                        insertRecord.put(KEY_BEFORE, new ArrayList<>(Collections.singletonList(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                            data.put("id", "1");
                            return data;
                        }).get())));
                        after.put("value", insertRecord);
                        return after;
                    }).get())
            );

            BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = Mockito.mock(BiConsumer.class);
            HazelcastProcessorBaseNode.ProcessResult processResult = new HazelcastProcessorBaseNode.ProcessResult();

            mockIns.process(tapdataEvent, consumer, processResult);
            Mockito.verify(mockIns, Mockito.times(1)).decodeDeleteRecord(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        }
    }

    @Nested
    class GetValueTest {
        @Test
        void testNotJsonValue() {
            Map<String, Object> mockValue = new LinkedHashMap<>();
            mockValue.put("value", "100");

            Assertions.assertThrows(RuntimeException.class, () -> ins.getValue(mockValue));
        }
    }
}
