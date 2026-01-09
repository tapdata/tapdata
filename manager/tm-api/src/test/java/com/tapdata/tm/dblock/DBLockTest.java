package com.tapdata.tm.dblock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DBLock 工具类测试
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/12 09:09 Create
 */
@DisplayName("DBLock 工具类测试")
class DBLockTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DBLockTest.class);

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

    @Test
    @DisplayName("测试线程执行完后释放，防止堆外内存无限增长")
    void testThreadRelease() throws Exception {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        int threadCount = 50;
        int corePoolSize = 2;
        long keepAliveTime = 2000L;
        long timeout = System.currentTimeMillis() + keepAliveTime * 3;

        ScheduledExecutorService executorService = DBLock.initExecutorService(corePoolSize, keepAliveTime);
        assertNotNull(executorService);
        if (executorService instanceof ScheduledThreadPoolExecutor executor) {
            assertTrue(executor.allowsCoreThreadTimeOut());
        } else {
            fail("非 ScheduledThreadPoolExecutor 类型");
        }

        // 模拟线程执行
        AtomicInteger completedCount = new AtomicInteger(0);
        for (int i = 0; i < threadCount; i++) {
            executorService.execute(() -> {
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    completedCount.incrementAndGet();
                }
            });
        }

        List<String> threadNames = new ArrayList<>();
        do {
            assertTrue(timeout > System.currentTimeMillis(), "未在规定时间内释放所有线程");

            threadNames.clear();
            Optional.of(threadMXBean.getAllThreadIds())
                .map(ids -> threadMXBean.getThreadInfo(ids, 0))
                .ifPresent(infos -> {
                    for (ThreadInfo info : infos) {
                        if (null == info) continue; // 已销毁
                        if (info.getThreadName().startsWith(DBLock.TAG)) {
                            threadNames.add(info.getThreadName());
                        }
                    }
                });
            LOGGER.info("等待线程释放（{}/{}）: {}", completedCount.get(), threadCount, threadNames);
            TimeUnit.SECONDS.sleep(1);
        } while (!threadNames.isEmpty());

        assertEquals(threadCount, completedCount.get(), "部分线程未被执行");
    }
}
