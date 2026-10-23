package io.tapdata.flow.engine.V2.util;

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
    void mysqlDateToOracleDateUsesJdbcDateAndPreservesCalendarDay() {
        TapCodecsFilterManager manager = TapCodecUtil.createEngineCodecsFilterManger();
        LocalDateTime sourceDate = LocalDateTime.of(2026, 10, 21, 0, 0);
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("birth_date", new TapDateValue(new DateTime(sourceDate)));
        Map<String, TapField> targetFields = Collections.singletonMap(
                "birth_date", new TapField("birth_date", "DATE"));

        manager.transformFromTapValueMap(values, targetFields, targetFields);

        assertInstanceOf(java.sql.Date.class, values.get("birth_date"));
        assertEquals(java.sql.Date.valueOf(sourceDate.toLocalDate()), values.get("birth_date"));
    }

    @Test
    void mssqlDatetime2SevenToDatetimeUsesTargetFractionScale() {
        TapCodecsFilterManager manager = TapCodecUtil.createEngineCodecsFilterManger();
        LocalDateTime sourceDateTime = LocalDateTime.of(2026, 10, 21, 12, 13, 14, 123456789);
        TapDateTimeValue sourceValue = new TapDateTimeValue(new DateTime(sourceDateTime));
        sourceValue.setTapType(new TapDateTime().fraction(7));
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("event_time", sourceValue);
        Map<String, TapField> targetFields = Collections.singletonMap(
                "event_time", new TapField("event_time", "datetime").scale(3));

        manager.transformFromTapValueMap(values, targetFields, targetFields);

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
