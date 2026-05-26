package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbSqlConfigTest {

    @AfterEach
    void tearDown() {
        // 每个测试后重置配置
        DuckDbSqlConfig.resetToDefault();
    }

    @Test
    void testDefault_IsTrue() {
        assertTrue(DuckDbSqlConfig.isUseNewWideTableUpdater());
    }

    @Test
    void testSetToFalse() {
        DuckDbSqlConfig.setUseNewWideTableUpdater(false);
        assertFalse(DuckDbSqlConfig.isUseNewWideTableUpdater());
    }

    @Test
    void testSetToTrue() {
        DuckDbSqlConfig.setUseNewWideTableUpdater(true);
        assertTrue(DuckDbSqlConfig.isUseNewWideTableUpdater());
    }

    @Test
    void testResetToDefault() {
        DuckDbSqlConfig.setUseNewWideTableUpdater(false);
        DuckDbSqlConfig.resetToDefault();
        // 默认应该为 true（除非设置了环境变量）
        assertTrue(DuckDbSqlConfig.isUseNewWideTableUpdater());
    }
}
