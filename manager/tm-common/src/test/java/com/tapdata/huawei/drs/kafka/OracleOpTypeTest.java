package com.tapdata.huawei.drs.kafka;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/27 10:45 Create
 */
class OracleOpTypeTest {

    @Nested
    class FromValueTest {

        @Test
        void testInsert() {
            Assertions.assertEquals(OracleOpType.INSERT, OracleOpType.fromValue("INSERT"));
        }

        @Test
        void testUpdate() {
            Assertions.assertEquals(OracleOpType.UPDATE, OracleOpType.fromValue("UPDATE"));
        }

        @Test
        void testDelete() {
            Assertions.assertEquals(OracleOpType.DELETE, OracleOpType.fromValue("DELETE"));
        }

        @Test
        void testDDL() {
            Assertions.assertEquals(OracleOpType.DDL, OracleOpType.fromValue("DDL"));
        }

        @Test
        void testUndefined() {
            Assertions.assertEquals(OracleOpType.UNDEFINED, OracleOpType.fromValue("-"));
        }
    }

}
