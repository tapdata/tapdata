package com.tapdata.constant;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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
}
