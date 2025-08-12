package com.tapdata.tm.dblock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DBLock 工具类测试
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/12 09:09 Create
 */
@DisplayName("DBLock 工具类测试")
class DBLockTest {

    @BeforeEach
    void setUp() {
    }

    @Test
    @DisplayName("测试 prefixTag 方法 - 无参数")
    void testPrefixTagWithoutArgs() {
        String msg = " test message";
        String result = DBLock.prefixTag(msg);
        assertNotNull(result, "prefixTag 应该返回非 null 的字符串");
        assertTrue(result.contains(DBLock.TAG), "prefixTag 应该包含 TAG 前缀");
        assertTrue(result.contains(msg), "prefixTag 应该包含传入的消息");
    }

    @Test
    @DisplayName("测试 prefixTag 方法 - 带参数")
    void testPrefixTagWithArgs() {
        String result = DBLock.prefixTag(" lock key '%s' initialized", "test-key");
        assertEquals(DBLock.TAG + " lock key 'test-key' initialized", result, "prefixTag 应该正确格式化带参数的消息");
    }

    @Test
    @DisplayName("测试 prefixTag 方法 - 多个参数")
    void testPrefixTagWithMultipleArgs() {
        String result = DBLock.prefixTag(" %s:%d:%s", "host", 8080, "thread");
        assertEquals(DBLock.TAG + " host:8080:thread", result, "prefixTag 应该正确格式化多种类型的参数");
    }

    @Test
    @DisplayName("测试 executor 子线程命名")
    void testExecutorThreadNaming() {
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> Thread.currentThread().getName(), DBLock.executor);
        String result = assertDoesNotThrow(() -> future.get(), "CompletableFuture.get() 不应该抛出异常");
        assertTrue(result.startsWith(DBLock.TAG), "子线程名称应该以 " + DBLock.TAG + "- 开头");
    }
}
