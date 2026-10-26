package io.tapdata.flow.engine.V2.util;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class TapCodecUtilTargetTypeTest {
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

        TapCodecUtil.convertForPhysicalTarget(values, logicalFields, physicalFields, null, registry);
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

        TapCodecUtil.convertForPhysicalTarget(values, logicalFields, physicalFields, null, registry);
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

        TapCodecUtil.convertForPhysicalTarget(values, logicalFields, physicalFields, null, registry);
        manager.transformFromTapValueMap(values, logicalFields, physicalFields);

        assertEquals("connector-v2:DATE", values.get("BIRTH_DATE"));
    }

    @Test
    void physicalDateTimeConversionPrecedesConnectorLegacyCodec() {
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

        TapCodecUtil.convertForPhysicalTarget(values, logicalFields, physicalFields,
                Collections.singleton("EVENT_TIME"), registry);
        manager.transformFromTapValueMap(values, logicalFields, physicalFields);

        assertInstanceOf(Timestamp.class, values.get("EVENT_TIME"));
        assertEquals(123000000, ((Timestamp) values.get("EVENT_TIME")).getNanos());

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

        assertInstanceOf(Timestamp.class, values.get("event_time"));
        assertEquals(123000000, ((Timestamp) values.get("event_time")).getNanos());
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
