package com.tapdata.constant;

import io.tapdata.entity.schema.value.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;

/**
 * 日期处理工具类测试
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/10/11 15:25 Create
 */
public class DateUtilTest {

    @Nested
    class GetTimeZoneTest {

        @Test
        void testNullArgs() {
            TimeZone defZone = TimeZone.getDefault();
            TimeZone timeZone = DateUtil.getTimeZone(null);
            Assertions.assertEquals(defZone.getRawOffset(), timeZone.getRawOffset());
        }

        @Test
        void testNormal() {
            for (int i = -12; i <= 12; i++) {
                TimeZone timeZone = DateUtil.getTimeZone(i);
                Assertions.assertEquals(i * 3600000, timeZone.getRawOffset());
            }
        }

        @Test
        void testIllegal() {
            int offset = -13;
            TimeZone timeZone = DateUtil.getTimeZone(offset);
            Assertions.assertEquals(offset * 3600000, timeZone.getRawOffset());

            offset = 13;
            timeZone = DateUtil.getTimeZone(offset);
            Assertions.assertEquals(offset * 3600000, timeZone.getRawOffset());
        }

    }

    @Nested
    @DisplayName("Object-aware operations")
    class ObjectAwareOperationsTest {

        @Test
        @DisplayName("addYears should accept DateTime and return DateTime")
        void testAddYearsSupportsDateTime() {
            DateTime source = new DateTime(Instant.parse("2024-03-31T12:30:00Z"));
            source.setTimeZone(TimeZone.getTimeZone("GMT+8"));

            DateTime result = DateUtil.addYears(source, 1);

            Assertions.assertEquals(Instant.parse("2025-03-31T12:30:00Z"), result.toInstant());
            Assertions.assertEquals("GMT+08:00", result.getTimeZone().getID());
            Assertions.assertEquals(Instant.parse("2024-03-31T12:30:00Z"), source.toInstant());
        }

        @Test
        @DisplayName("compare helpers should accept mixed temporal types")
        void testCompareHelpersSupportMixedTypes() {
            DateTime left = new DateTime(Instant.parse("2024-03-01T00:00:00Z"));
            Date middle = Date.from(Instant.parse("2024-03-01T00:00:00Z"));
            Instant right = Instant.parse("2024-03-02T00:00:00Z");

            Assertions.assertEquals(0, DateUtil.compare(left, middle));
            Assertions.assertTrue(DateUtil.isBefore(left, right));
            Assertions.assertTrue(DateUtil.isAfter(right, middle));
            Assertions.assertTrue(DateUtil.isEqual(left, middle));
        }

        @Test
        @DisplayName("same day and diff helpers should work with object arguments")
        void testSameDayAndDiffHelpersSupportObjectArgs() {
            DateTime left = new DateTime(Instant.parse("2024-03-31T23:30:00Z"));
            Instant right = Instant.parse("2024-04-01T00:15:00Z");

            Assertions.assertTrue(DateUtil.isSameDay(left, right, ZoneId.of("Asia/Shanghai")));
            Assertions.assertFalse(DateUtil.isSameDay(left, right, ZoneId.of("UTC")));
            Assertions.assertEquals(-2700000L, DateUtil.diffMillis(left, right));
            Assertions.assertEquals(-45L, DateUtil.diffMinutes(left, right));
        }
    }
}
