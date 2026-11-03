package io.tapdata.flow.engine.V2.util;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.codec.filter.TapCodecsFilterManagerSchemaEnforced;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class TapCodecUtilTargetTypeTest {
    @Test
    void stringCodecRetainsDaylightSavingTimeConversion() {
        TapCodecsRegistry registry = TapCodecsRegistry.create();
        AtomicInteger calls = new AtomicInteger();
        registry.registerFromTapValue(TapDateTimeValue.class, value -> {
                calls.incrementAndGet();
                return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                        .withZone(ZoneId.of("America/New_York"))
                        .format(value.getValue().toInstant());
        });
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("EVENT_TIME", new TapDateTimeValue(new DateTime(
                Instant.parse("2026-07-01T12:00:00.123456Z"))));
        Map<String, TapField> logical = Collections.singletonMap(
                "EVENT_TIME", new TapField("EVENT_TIME", "datetime(6)").scale(6));
        Map<String, TapField> physical = Collections.singletonMap(
                "EVENT_TIME", new TapField("EVENT_TIME", "datetime").scale(3));

        TapCodecUtil.convertForPhysicalTarget(values, logical, physical, registry);
        TapCodecsFilterManager.create(registry).transformFromTapValueMap(values, logical, physical);

        assertEquals("2026-07-01 08:00:00.123456", values.get("EVENT_TIME"));
        assertEquals(1, calls.get());

        Map<String, Object> cdcValues = new LinkedHashMap<>();
        cdcValues.put("EVENT_TIME", new TapDateTimeValue(new DateTime(
                Instant.parse("2026-07-01T12:00:00.123456Z"))));
        TapCodecUtil.convertForPhysicalTarget(cdcValues, logical, physical, registry);
        new TapCodecsFilterManagerSchemaEnforced(registry)
                .transformFromTapValueMap(cdcValues, logical, physical, Collections.emptySet());
        assertEquals("2026-07-01 08:00:00.123456", cdcValues.get("EVENT_TIME"));
        assertEquals(2, calls.get());
    }

    @Test
    void physicalDateConversionPrecedesConnectorLegacyCodec() {
        TapCodecsRegistry registry = TapCodecsRegistry.create();
        registry.registerFromTapValue(TapDateValue.class, "CHAR(10)", value -> "legacy-date");
        TapCodecsFilterManager manager = TapCodecsFilterManager.create(registry);
        Map<String, TapField> logicalFields = Collections.singletonMap(
                "BIRTH_DATE", new TapField("BIRTH_DATE", "CHAR(10)"));
        Map<String, TapField> physicalFields = Collections.singletonMap(
                "BIRTH_DATE", new TapField("BIRTH_DATE", "DATE"));
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("BIRTH_DATE", new TapDateValue(new DateTime(LocalDateTime.of(2026, 10, 21, 0, 0))));

        TapCodecUtil.convertForPhysicalTarget(values, logicalFields, physicalFields, registry);
        manager.transformFromTapValueMap(values, logicalFields, physicalFields);

        assertEquals(java.sql.Date.valueOf("2026-10-21"), values.get("BIRTH_DATE"));
        assertEquals("CHAR(10)", logicalFields.get("BIRTH_DATE").getDataType());

        values.put("BIRTH_DATE", new TapDateValue(new DateTime(LocalDateTime.of(2026, 10, 21, 0, 0))));
        manager.transformFromTapValueMap(values, logicalFields, logicalFields);
        assertEquals("legacy-date", values.get("BIRTH_DATE"));
    }

    @Test
    void matchingLogicalAndPhysicalDateKeepsConnectorCodec() {
        TapCodecsRegistry registry = TapCodecsRegistry.create();
        registry.registerFromTapValue(TapDateValue.class, value -> "legacy-date");
        TapCodecsFilterManager manager = TapCodecsFilterManager.create(registry);
        Map<String, TapField> logicalFields = Collections.singletonMap(
                "BIRTH_DATE", new TapField("BIRTH_DATE", "DATE"));
        Map<String, TapField> physicalFields = Collections.singletonMap(
                "BIRTH_DATE", new TapField("BIRTH_DATE", "DATE"));
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("BIRTH_DATE", new TapDateValue(new DateTime(LocalDateTime.of(2026, 10, 21, 0, 0))));

        TapCodecUtil.convertForPhysicalTarget(values, logicalFields, physicalFields, registry);
        manager.transformFromTapValueMap(values, logicalFields, physicalFields);

        assertEquals("legacy-date", values.get("BIRTH_DATE"));
    }

    @Test
    void connectorTargetAwareCodecKeepsPriorityForPhysicalDate() {
        TapCodecsRegistry registry = TapCodecsRegistry.create();
        registry.registerFromTapValueV2(TapDateValue.class,
                (value, targetField) -> "connector-v2:" + targetField.getDataType());
        TapCodecsFilterManager manager = TapCodecsFilterManager.create(registry);
        Map<String, TapField> logicalFields = Collections.singletonMap(
                "BIRTH_DATE", new TapField("BIRTH_DATE", "CHAR(10)"));
        Map<String, TapField> physicalFields = Collections.singletonMap(
                "BIRTH_DATE", new TapField("BIRTH_DATE", "DATE"));
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("BIRTH_DATE", new TapDateValue(new DateTime(LocalDateTime.of(2026, 10, 21, 0, 0))));

        TapCodecUtil.convertForPhysicalTarget(values, logicalFields, physicalFields, registry);
        manager.transformFromTapValueMap(values, logicalFields, physicalFields);

        assertEquals("connector-v2:DATE", values.get("BIRTH_DATE"));
    }

    @Test
    void physicalDateTimeKeepsUnsupportedConnectorLegacyCodec() {
        TapCodecsRegistry registry = TapCodecsRegistry.create();
        registry.registerFromTapValue(TapDateTimeValue.class, value -> "legacy-datetime");
        TapCodecsFilterManager manager = TapCodecsFilterManager.create(registry);
        Map<String, TapField> logicalFields = Collections.singletonMap(
                "EVENT_TIME", new TapField("EVENT_TIME", "datetime2").scale(7));
        Map<String, TapField> physicalFields = Collections.singletonMap(
                "EVENT_TIME", new TapField("EVENT_TIME", "datetime").scale(3));
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("EVENT_TIME", new TapDateTimeValue(new DateTime(
                LocalDateTime.of(2026, 10, 21, 12, 13, 14, 123456789))));

        TapCodecUtil.convertForPhysicalTarget(values, logicalFields, physicalFields, registry);
        manager.transformFromTapValueMap(values, logicalFields, physicalFields);

        assertEquals("legacy-datetime", values.get("EVENT_TIME"));

        values.put("EVENT_TIME", new TapDateTimeValue(new DateTime(
                LocalDateTime.of(2026, 10, 21, 12, 13, 14, 123456789))));
        manager.transformFromTapValueMap(values, logicalFields, logicalFields);
        assertEquals("legacy-datetime", values.get("EVENT_TIME"));
    }

    @Test
    void mysqlDateToOracleDateUsesJdbcDateAndPreservesCalendarDay() {
        TapCodecsFilterManager manager = TapCodecUtil.createEngineCodecsFilterManger();
        LocalDateTime sourceDate = LocalDateTime.of(2026, 10, 21, 0, 0);
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("birth_date", new TapDateValue(new DateTime(sourceDate)));
        Map<String, TapField> logicalFields = Collections.singletonMap(
                "birth_date", new TapField("birth_date", "CHAR(10)"));
        Map<String, TapField> physicalTargetFields = Collections.singletonMap(
                "birth_date", new TapField("birth_date", "DATE"));

        manager.transformFromTapValueMap(values, logicalFields, physicalTargetFields);

        assertInstanceOf(java.sql.Date.class, values.get("birth_date"));
        assertEquals(java.sql.Date.valueOf(sourceDate.toLocalDate()), values.get("birth_date"));
        assertEquals("CHAR(10)", logicalFields.get("birth_date").getDataType());
    }

    @Test
    void mssqlDatetime2SevenToDatetimeUsesTargetFractionScale() {
        TapCodecsFilterManager manager = TapCodecUtil.createEngineCodecsFilterManger();
        LocalDateTime sourceDateTime = LocalDateTime.of(2026, 10, 21, 12, 13, 14, 123456789);
        TapDateTimeValue sourceValue = new TapDateTimeValue(new DateTime(sourceDateTime));
        sourceValue.setTapType(new TapDateTime().fraction(7));
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("event_time", sourceValue);
        Map<String, TapField> logicalFields = Collections.singletonMap(
                "event_time", new TapField("event_time", "datetime2").scale(7));
        Map<String, TapField> physicalTargetFields = Collections.singletonMap(
                "event_time", new TapField("event_time", "datetime").scale(3));

        manager.transformFromTapValueMap(values, logicalFields, physicalTargetFields);

        assertEquals(LocalDateTime.of(2026, 10, 21, 12, 13, 14, 123000000),
                assertInstanceOf(LocalDateTime.class, values.get("event_time")));
    }

    @Test
    void mssqlDatetimePreservesWallClockInShanghaiTimeZone() {
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            TapDateTimeValue sourceValue = new TapDateTimeValue(new DateTime(
                    LocalDateTime.of(2026, 10, 21, 12, 13, 14, 123456700)));
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("event_time", sourceValue);
            Map<String, TapField> logicalFields = Collections.singletonMap(
                    "event_time", new TapField("event_time", "datetime2").scale(7));
            Map<String, TapField> physicalFields = Collections.singletonMap(
                    "event_time", new TapField("event_time", "datetime").scale(3));

            TapCodecUtil.convertForPhysicalTarget(values, logicalFields, physicalFields,
                    TapCodecsRegistry.create());

            assertEquals(LocalDateTime.of(2026, 10, 21, 12, 13, 14, 123000000),
                    assertInstanceOf(LocalDateTime.class, values.get("event_time")));
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    void preconvertedCdcDateTimeUsesPhysicalTargetEvenWithNoNewlyConvertedFields() {
        TapDateTimeValue value = new TapDateTimeValue(new DateTime(
                LocalDateTime.of(2026, 10, 22, 16, 17, 18, 123456700)));
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("EVENT_ID", 1);
        values.put("EVENT_TIME", value);
        Map<String, TapField> logicalFields = Collections.singletonMap(
                "EVENT_TIME", new TapField("EVENT_TIME", "datetime2").scale(7));
        Map<String, TapField> physicalFields = Collections.singletonMap(
                "EVENT_TIME", new TapField("EVENT_TIME", "datetime").scale(3));

        TapCodecUtil.convertForPhysicalTarget(values, logicalFields, physicalFields,
                TapCodecsRegistry.create());
        new TapCodecsFilterManagerSchemaEnforced(TapCodecsRegistry.create())
                .transformFromTapValueMap(values, logicalFields, physicalFields, Collections.emptySet());

        LocalDateTime converted = assertInstanceOf(LocalDateTime.class, values.get("EVENT_TIME"));
        assertEquals(LocalDateTime.of(2026, 10, 22, 16, 17, 18, 123000000), converted);
        assertEquals(1, values.get("EVENT_ID"));
    }

    @Test
    void physicalDatetimeKeepsConnectorTimezoneConversionBeforeReducingPrecision() {
        TapCodecsRegistry registry = TapCodecsRegistry.create();
        registry.registerFromTapValue(TapDateTimeValue.class, value ->
                LocalDateTime.of(2026, 10, 22, 16, 17, 18, 123456700));
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("EVENT_TIME", new TapDateTimeValue(new DateTime(
                LocalDateTime.of(2026, 10, 22, 8, 17, 18, 123456700))));
        Map<String, TapField> logicalFields = Collections.singletonMap(
                "EVENT_TIME", new TapField("EVENT_TIME", "datetime2").scale(7));
        Map<String, TapField> physicalFields = Collections.singletonMap(
                "EVENT_TIME", new TapField("EVENT_TIME", "datetime").scale(3));

        TapCodecUtil.convertForPhysicalTarget(values, logicalFields, physicalFields, registry);

        assertEquals(LocalDateTime.of(2026, 10, 22, 16, 17, 18, 123000000),
                assertInstanceOf(LocalDateTime.class, values.get("EVENT_TIME")));
    }

    @Test
    void missingTargetFieldKeepsLegacyDatetimeConversion() {
        TapCodecsFilterManager manager = TapCodecUtil.createEngineCodecsFilterManger();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("event_time", new TapDateTimeValue(
                new DateTime(LocalDateTime.of(2026, 10, 21, 12, 13, 14, 123456789))));

        manager.transformFromTapValueMap(values);

        assertInstanceOf(java.time.Instant.class, values.get("event_time"));
    }
}
