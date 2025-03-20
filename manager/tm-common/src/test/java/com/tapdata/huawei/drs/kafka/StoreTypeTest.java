package com.tapdata.huawei.drs.kafka;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/27 10:45 Create
 */
class StoreTypeTest {

    @Nested
    class FromValueTest {

        @Test
        void testDefault() {
            Assertions.assertEquals(StoreType.JSON, StoreType.fromValue(null));
            Assertions.assertEquals(StoreType.JSON, StoreType.fromValue(""));
        }

        @Test
        void testAvro() {
            Assertions.assertEquals(StoreType.AVRO, StoreType.fromValue("AVRO"));
        }

        @Test
        void testJson() {
            Assertions.assertEquals(StoreType.JSON, StoreType.fromValue("JSON"));
        }

        @Test
        void testJsonC() {
            Assertions.assertEquals(StoreType.JSON_C, StoreType.fromValue("JSON_C"));
        }

        @Test
        void testUndefined() {
            Assertions.assertEquals(StoreType.UNDEFINED, StoreType.fromValue("-"));
            Assertions.assertEquals(StoreType.UNDEFINED, StoreType.fromValue("UNDEFINED"));
        }

    }

}
