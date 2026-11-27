package io.tapdata.observable.metric.throttle;

import com.tapdata.tm.commons.throttle.DmlType;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.value.ByteData;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapStringValue;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Exact parity tests between {@link EventV1ReferenceSerializer} and zero-allocation {@link EventCostMeter} (AC-03).
 */
class EventV1MeterTest {

    private final EventV1ReferenceSerializer serializer = new EventV1ReferenceSerializer();
    private final EventCostMeter meter = new EventCostMeter();

    @Test
    void matchesReferenceDesignVectorsAndNullDistinction() {
        Map<String, Object> afterSimple = Map.of("id", 1);
        EventV1View vec76 = new EventV1View(DmlType.INSERT, "t", 0L, null, afterSimple);
        byte[] bytes76 = serializer.serialize(vec76);
        assertEquals("{\"type\":\"INSERT\",\"table\":\"t\",\"eventTimeMs\":0,\"before\":null,\"after\":{\"id\":1}}",
                new String(bytes76, StandardCharsets.UTF_8));
        assertEquals(76L, bytes76.length);
        assertEquals(76L, meter.measure(vec76));

        EventV1View vec78 = new EventV1View(DmlType.INSERT, "表", 0L, null, afterSimple);
        byte[] bytes78 = serializer.serialize(vec78);
        assertEquals(78L, bytes78.length);
        assertEquals(78L, meter.measure(vec78));

        Map<String, Object> emptyMap = Collections.emptyMap();
        Map<String, Object> explicitNullMap = new LinkedHashMap<>();
        explicitNullMap.put("x", null);

        EventV1View emptyAfterView = new EventV1View(DmlType.INSERT, "t", 0L, null, emptyMap);
        EventV1View explicitNullView = new EventV1View(DmlType.INSERT, "t", 0L, null, explicitNullMap);
        // Empty map "{}" (2B) vs {"x":null} (10B) -> diff strictly 8 bytes
        long emptyBytes = meter.measure(emptyAfterView);
        long nullKeyBytes = meter.measure(explicitNullView);
        assertEquals(8L, nullKeyBytes - emptyBytes);
        assertParity(emptyAfterView);
        assertParity(explicitNullView);

        // Whole absent before image ("null", 4B) vs empty before map ("{}", 2B)
        EventV1View emptyBeforeView = new EventV1View(DmlType.UPDATE, "t", 0L, emptyMap, emptyMap);
        EventV1View absentBeforeView = new EventV1View(DmlType.UPDATE, "t", 0L, null, emptyMap);
        assertEquals(2L, meter.measure(absentBeforeView) - meter.measure(emptyBeforeView));
        assertParity(emptyBeforeView);
        assertParity(absentBeforeView);
    }

    @Test
    void matchesParityAcrossEscapesMultibyteSurrogatesTypesAndUnorderedMaps() {
        Map<String, Object> m1 = new LinkedHashMap<>();
        m1.put("z_esc", "quote:\" slash:\\ b:\b f:\f n:\n r:\r t:\t ctrl:\u0001\u001f");
        m1.put("a_utf8", "中文😀\uD83D\uDE80_surrogate_" + "\uD800");
        m1.put("b_nums", Arrays.asList(
                (byte) -128, (short) 32000, Integer.MIN_VALUE, Long.MAX_VALUE,
                new BigInteger("99999999999999999999999999"),
                new BigDecimal("1234567890.00100"),
                -0.0d, 0.0d, 3.14159d, Double.NaN, Double.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY));
        m1.put("c_bins", Arrays.asList(
                new byte[0],
                new byte[]{1, 2, 3, 4, 5},
                ByteBuffer.wrap(new byte[]{9, 8, 7, 6}, 1, 2),
                new ByteData(new byte[]{11, 22}),
                new Binary(new byte[]{33, 44, 55})));

        DateTime illegalDt = new DateTime("0000-00-00 00:00:00", DateTime.DATETIME_TYPE);
        DateTime normalDt = new DateTime(Instant.ofEpochSecond(1_700_000_000L, 123_456_789));
        m1.put("d_times", Arrays.asList(
                normalDt,
                illegalDt,
                Instant.ofEpochMilli(123_456L),
                new Date(123_456L),
                new Timestamp(123_456L),
                OffsetDateTime.of(2026, 11, 25, 10, 0, 0, 0, ZoneOffset.ofHours(8)),
                LocalDate.of(2026, 11, 25),
                LocalTime.of(12, 34, 56),
                LocalDateTime.of(2026, 11, 25, 12, 34, 56)));

        TapMapValue nestedTapMap = new TapMapValue(Map.of("k2", new TapStringValue("v2"), "k1", new TapNumberValue(42.5d)));
        TapRawValue originOnlyTap = new TapRawValue();
        originOnlyTap.originValue("originVal");
        m1.put("e_tapValues", Arrays.asList(
                new TapBinaryValue(new byte[]{7, 8, 9}),
                new TapDateTimeValue(normalDt),
                nestedTapMap,
                originOnlyTap,
                new ObjectId("0123456789abcdef01234567"),
                UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
                new int[]{1, 2, 3},
                Arrays.asList(null, "keep-null-slot", null)));

        // Reversed key order should produce identical byte count even without sorting in EventCostMeter
        Map<String, Object> m1Reversed = new LinkedHashMap<>();
        List<String> keys = new java.util.ArrayList<>(m1.keySet());
        Collections.reverse(keys);
        for (String k : keys) {
            m1Reversed.put(k, m1.get(k));
        }

        TapUpdateRecordEvent update = new TapUpdateRecordEvent();
        update.table("orders_utf8_表");
        update.referenceTime(1700000123456L);
        update.before(m1);
        update.after(m1Reversed);

        EventV1View view = EventV1View.fromTapRecordEvent(update, "normalized.orders_表");
        long len1 = assertParity(view);

        TapDeleteRecordEvent del = new TapDeleteRecordEvent();
        del.table("orders");
        del.setTime(999L);
        del.before(m1Reversed);
        assertParity(EventV1View.fromTapRecordEvent(del, null));

        // Reordered top-level maps yield exact same length
        EventV1View viewSwapped = new EventV1View(DmlType.UPDATE, "normalized.orders_表", 1700000123456L, m1Reversed, m1);
        assertEquals(len1, meter.measure(viewSwapped));
    }

    @Test
    void enforcesDepthWorkBudgetAndCircularReferences() {
        // Circular reference check
        Map<String, Object> cyclic = new HashMap<>();
        cyclic.put("self", cyclic);
        EventV1View cyclicView = new EventV1View(DmlType.INSERT, "t", 0L, null, cyclic);
        assertThrows(MeterBudgetExceededException.class, () -> meter.measure(cyclicView));
        assertThrows(MeterBudgetExceededException.class, () -> serializer.serialize(cyclicView));

        // Max depth check
        Map<String, Object> deep = new HashMap<>();
        Map<String, Object> cur = deep;
        for (int i = 0; i < 10; i++) {
            Map<String, Object> next = new HashMap<>();
            cur.put("n", next);
            cur = next;
        }
        EventCostMeter shallowMeter = new EventCostMeter(5, 1000);
        EventV1View deepView = new EventV1View(DmlType.INSERT, "t", 0L, null, deep);
        assertThrows(MeterBudgetExceededException.class, () -> shallowMeter.measure(deepView));

        // Max work units check
        TapInsertRecordEvent large = new TapInsertRecordEvent().table("t").referenceTime(0L);
        Map<String, Object> manyFields = new HashMap<>();
        for (int i = 0; i < 50; i++) {
            manyFields.put("f" + i, i);
        }
        large.after(manyFields);
        EventV1View manyView = EventV1View.fromTapRecordEvent(large, "t");
        assertThrows(MeterBudgetExceededException.class, () -> meter.measure(manyView, 10));
    }

    private long assertParity(EventV1View view) {
        byte[] serialized = serializer.serialize(view);
        long counted = meter.measure(view);
        assertEquals(serialized.length, counted, "Byte length mismatch for " + new String(serialized, StandardCharsets.UTF_8));
        return counted;
    }
}
