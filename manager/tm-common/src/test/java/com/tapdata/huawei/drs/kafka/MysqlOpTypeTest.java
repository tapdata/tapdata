package com.tapdata.huawei.drs.kafka;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/27 10:45 Create
 */
class MysqlOpTypeTest {

    @Nested
    class FromValueTest {

        @Test
        void testInsert() {
            Assertions.assertEquals(MysqlOpType.INSERT, MysqlOpType.fromValue("INSERT"));
        }

        @Test
        void testUpdate() {
            Assertions.assertEquals(MysqlOpType.UPDATE, MysqlOpType.fromValue("UPDATE"));
        }

        @Test
        void testDelete() {
            Assertions.assertEquals(MysqlOpType.DELETE, MysqlOpType.fromValue("DELETE"));
        }

        @Test
        void testInit() {
            Assertions.assertEquals(MysqlOpType.INIT, MysqlOpType.fromValue("INIT"));
        }

        @Test
        void testInitDdl() {
            Assertions.assertEquals(MysqlOpType.INIT_DDL, MysqlOpType.fromValue("INIT_DDL"));
        }

        @Test
        void testDDL() {
            Assertions.assertEquals(MysqlOpType.DDL, MysqlOpType.fromValue("DDL"));
        }

        @Test
        void testUndefined() {
            Assertions.assertEquals(MysqlOpType.UNDEFINED, MysqlOpType.fromValue("-"));
        }
    }

    @Test
    void testGetStage() {
        for (MysqlOpType type : MysqlOpType.values()) {
            if (MysqlOpType.UNDEFINED == type) {
                Assertions.assertNull(type.getStage());
            } else {
                Assertions.assertNotNull(type.getStage());
            }
        }
    }
}
