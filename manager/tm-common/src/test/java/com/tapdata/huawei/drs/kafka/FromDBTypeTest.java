package com.tapdata.huawei.drs.kafka;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/27 10:55 Create
 */
class FromDBTypeTest {

    @Nested
    class FromValueTest {
        @Test
        void testMysql() {
            Assertions.assertEquals(FromDBType.MYSQL, FromDBType.fromValue("MYSQL"));
        }

        @Test
        void testGaussDBMysql() {
            Assertions.assertEquals(FromDBType.GAUSSDB_MYSQL, FromDBType.fromValue("GAUSSDB_MYSQL"));
        }

        @Test
        void testGaussDB() {
            Assertions.assertEquals(FromDBType.GAUSSDB, FromDBType.fromValue("GAUSSDB"));
        }

        @Test
        void testOracle() {
            Assertions.assertEquals(FromDBType.ORACLE, FromDBType.fromValue("ORACLE"));
        }

        @Test
        void testMssql() {
            Assertions.assertEquals(FromDBType.MSSQL, FromDBType.fromValue("MSSQL"));
        }

        @Test
        void testPostgresql() {
            Assertions.assertEquals(FromDBType.POSTGRESQL, FromDBType.fromValue("POSTGRESQL"));
        }

        @Test
        void testUndefined() {
            Assertions.assertEquals(FromDBType.UNDEFINED, FromDBType.fromValue("-"));
        }
    }

}
