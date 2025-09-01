package com.tapdata.tm.dblock.impl;

import com.tapdata.tm.commons.function.ThrowableConsumer;
import com.tapdata.tm.dblock.DBLock;
import com.tapdata.tm.dblock.ILock;
import com.tapdata.tm.dblock.repository.MemoryDBLockRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ActiveServiceLock 主备切换测试类
 *
 * @author Test
 * @version v1.0 2025/8/11 Create
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("ActiveServiceLock 主备切换测试")
class ActiveServiceLockTest {

    private MemoryDBLockRepository repository;
    private ILock mockLock;

    private final String testOwner = "test-service-instance";
    private final String testKey = "test-service-lock";
    private final long expireSeconds = 10L;
    private final long heartbeatSeconds = 2L;

    private ActiveServiceLock activeServiceLock;
    private AtomicInteger activeCallCount;
    private AtomicInteger standbyCallCount;

    @BeforeEach
    void setUp() {
        repository = new MemoryDBLockRepository();
        mockLock = DBLock.create(repository, testKey);

        activeCallCount = new AtomicInteger(0);
        standbyCallCount = new AtomicInteger(0);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (activeServiceLock != null) {
            activeServiceLock.close();
        }
    }

    @Test
    @DisplayName("测试构造函数和初始状态")
    void testConstructorAndInitialState() {
        ThrowableConsumer<ActiveServiceLock, Exception> doActive = lock -> activeCallCount.incrementAndGet();
        ThrowableConsumer<ActiveServiceLock, Exception> doStandby = lock -> standbyCallCount.incrementAndGet();

        activeServiceLock = new ActiveServiceLock(mockLock, testOwner, expireSeconds, heartbeatSeconds, doActive, doStandby);

        assertNotNull(activeServiceLock, "ActiveServiceLock 实例不应该为 null");
        assertFalse(activeServiceLock.isActive(), "初始状态应该是非活跃的");
        assertEquals(testKey, activeServiceLock.getKey(), "key 应该正确设置");
    }

    @Test
    @DisplayName("测试从备用状态切换到活跃状态")
    void testSwitchFromStandbyToActive() throws InterruptedException {
        repository.setOwner(testKey, "another-owner");
        repository.setExpireTime(testKey, new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1)));

        CountDownLatch activeLatch = new CountDownLatch(1);

        ThrowableConsumer<ActiveServiceLock, Exception> doActive = lock -> {
            activeCallCount.incrementAndGet();
            activeLatch.countDown();
        };
        ThrowableConsumer<ActiveServiceLock, Exception> doStandby = lock -> standbyCallCount.incrementAndGet();

        activeServiceLock = new ActiveServiceLock(mockLock, testOwner, expireSeconds, heartbeatSeconds, doActive, doStandby);

        // 等待心跳执行
        assertTrue(activeLatch.await(10, TimeUnit.SECONDS), "应该在超时前切换到活跃状态");

        assertTrue(activeServiceLock.isActive(), "应该处于活跃状态");
        assertEquals(1, activeCallCount.get(), "doActive 应该被调用一次");
        assertEquals(0, standbyCallCount.get(), "doStandby 不应该被调用");
    }

    @Test
    @DisplayName("测试从活跃状态切换到备用状态")
    void testSwitchFromActiveToStandby() throws InterruptedException {
        CountDownLatch activeLatch = new CountDownLatch(1);
        CountDownLatch standbyLatch = new CountDownLatch(1);

        ThrowableConsumer<ActiveServiceLock, Exception> doActive = lock -> {
            activeCallCount.incrementAndGet();
            activeLatch.countDown();
        };
        ThrowableConsumer<ActiveServiceLock, Exception> doStandby = lock -> {
            standbyCallCount.incrementAndGet();
            standbyLatch.countDown();
        };

        activeServiceLock = new ActiveServiceLock(mockLock, testOwner, expireSeconds, heartbeatSeconds, doActive, doStandby);

        // 等待切换到活跃状态
        assertTrue(activeLatch.await(10, TimeUnit.SECONDS), "应该先切换到活跃状态");
        assertTrue(activeServiceLock.isActive(), "应该处于活跃状态");

        repository.setOwner(testKey, "another-owner"); // 切换到其它节点，自动转备用

        // 等待切换到备用状态
        assertTrue(standbyLatch.await(10, TimeUnit.SECONDS), "应该切换到备用状态");
        assertFalse(activeServiceLock.isActive(), "应该处于备用状态");

        assertEquals(1, activeCallCount.get(), "doActive 应该被调用一次");
        assertEquals(1, standbyCallCount.get(), "doStandby 应该被调用一次");
    }

}
