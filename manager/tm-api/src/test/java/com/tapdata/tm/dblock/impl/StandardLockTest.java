package com.tapdata.tm.dblock.impl;

import com.tapdata.tm.commons.function.ThrowableConsumer;
import com.tapdata.tm.commons.function.ThrowableFunction;
import com.tapdata.tm.dblock.LockStateEnums;
import com.tapdata.tm.dblock.repository.MemoryDBLockRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * StandardLock 实现测试类
 *
 * @author Test
 * @version v1.0 2025/8/11 Create
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("StandardLock 实现测试")
class StandardLockTest {

    private MemoryDBLockRepository repository;

    private StandardLock standardLock;
    private final String testKey = "test-lock-key";
    private final String testOwner = "test-owner";
    private final long testTimeout = 10000L; // 10秒超时

    @BeforeEach
    void setUp() {
        repository = new MemoryDBLockRepository();
        standardLock = new StandardLock(repository, testKey);
    }

    @Test
    @DisplayName("测试构造函数 - 新锁初始化")
    void testConstructorWithNewLock() {
        assertEquals(repository, standardLock.getRepository(), "repository 应该正确设置");
        assertEquals(testKey, standardLock.getKey(), "key 应该正确设置");
        assertNotNull(repository.getLockData(testKey), "锁数据应该被初始化");
    }

    @Test
    @DisplayName("测试构造函数 - 已存在的锁")
    void testConstructorWithExistingLock() {
        StandardLock lock2 = new StandardLock(repository, testKey);

        assertEquals(repository, lock2.getRepository(), "repository 应该正确设置");
        assertEquals(testKey, lock2.getKey(), "key 应该正确设置");
        assertNotNull(repository.getLockData(testKey), "锁数据应该被初始化");
    }

    @Test
    @DisplayName("测试 acquire 方法 - 成功获取锁")
    void testAcquireSuccess() {
        LockStateEnums result = standardLock.acquire(testOwner, testTimeout);

        assertNotEquals(LockStateEnums.NO, result, "应该成功获取锁");
    }

    @Test
    @DisplayName("测试 acquire 方法 - 获取锁失败")
    void testAcquireFailure() {
        repository.setOwner(testKey, "another-owner");                           // 其他用户占用锁
        repository.setExpireTime(testKey, new Date(System.currentTimeMillis() + 5000)); // 锁未过期

        LockStateEnums result = standardLock.acquire(testOwner, testTimeout);

        assertEquals(LockStateEnums.NO, result, "应该获取锁失败");
    }

    @Test
    @DisplayName("测试 acquire 方法 - 变更拥有者")
    void testAcquireWithOwnerChange() {
        repository.setOwner(testKey, "another-owner");                           // 其他用户占用锁
        repository.setExpireTime(testKey, new Date(System.currentTimeMillis() - 1000)); // 锁已过期

        LockStateEnums result = standardLock.acquire(testOwner, testTimeout);

        assertEquals(LockStateEnums.YES_CHANGE, result, "应该返回变更拥有者状态");
    }

    @Test
    @DisplayName("测试 acquire 方法验证过期时间计算")
    void testAcquireExpirationTimeCalculation() {
        long beforeTime = System.currentTimeMillis();

        LockStateEnums state = standardLock.acquire(testOwner, testTimeout);
        MemoryDBLockRepository.LockData lockData = repository.getLockData(testKey);

        assertNotEquals(LockStateEnums.NO, state, "应该成功获取锁");
        assertNotNull(lockData, "锁数据应该被初始化");
        assertNotNull(lockData.getExpireTime(), "锁数据应该包含过期时间");
        assertTrue(lockData.getExpireTime().getTime() >= beforeTime + testTimeout, "锁数据过期时间应该在指定时间内");
        assertTrue(lockData.getExpireTime().getTime() < beforeTime + testTimeout + 1000, "时间误差1秒内");
    }

    @Test
    @DisplayName("测试 release 方法 - 成功释放")
    void testReleaseSuccess() {
        LockStateEnums state = standardLock.acquire(testOwner, testTimeout);
        boolean result = standardLock.release(testOwner);

        assertNotEquals(LockStateEnums.NO, state, "应该成功获取锁");
        assertTrue(result, "应该成功释放锁");
    }

    @Test
    @DisplayName("测试 release 方法 - 释放失败")
    void testReleaseFailure() {
        boolean result = standardLock.release(testOwner);

        assertFalse(result, "应该释放锁失败");
    }

    @Test
    @DisplayName("测试 callIfLocked 方法 - 获取锁成功")
    void testCallIfLockedSuccess() throws Exception {
        String expectedResult = "预期结果";

        ThrowableFunction<String, LockStateEnums, Exception> function = state -> {
            assertNotEquals(LockStateEnums.NO, state, "函数应该接收到正确的锁状态");
            return expectedResult;
        };

        String result = standardLock.callIfLocked(testOwner, function, testTimeout);

        assertEquals(expectedResult, result, "应该返回函数执行结果");
    }

    @Test
    @DisplayName("测试 callIfLocked 方法 - 获取锁失败")
    void testCallIfLockedFailure() throws Exception {
        repository.setOwner(testKey, "another-owner");                           // 其他用户占用锁
        repository.setExpireTime(testKey, new Date(System.currentTimeMillis() + 5000)); // 锁未过期
        ThrowableFunction<String, LockStateEnums, Exception> function = state -> {
            fail("函数不应该被执行");
            return "should not execute";
        };

        String result = standardLock.callIfLocked(testOwner, function, testTimeout);

        assertNull(result, "获取锁失败应该返回 null");
    }

    @Test
    @DisplayName("测试 callIfLocked 方法 - 函数抛出异常")
    void testCallIfLockedWithException() throws Exception {
        RuntimeException expectedException = new RuntimeException("test exception");

        ThrowableFunction<String, LockStateEnums, RuntimeException> function = state -> {
            throw expectedException;
        };

        RuntimeException thrownException = assertThrows(RuntimeException.class, () -> {
            standardLock.callIfLocked(testOwner, function, testTimeout);
        });

        assertEquals(expectedException, thrownException, "应该抛出函数中的异常");
    }

    @Test
    @DisplayName("测试 runIfLocked 方法 - 获取锁成功")
    void testRunIfLockedSuccess() throws Exception {
        AtomicBoolean executed = new AtomicBoolean(false);
        AtomicReference<LockStateEnums> receivedState = new AtomicReference<>();

        ThrowableConsumer<LockStateEnums, Exception> consumer = state -> {
            executed.set(true);
            receivedState.set(state);
        };

        standardLock.runIfLocked(testOwner, consumer, testTimeout);

        assertTrue(executed.get(), "消费者应该被执行");
        assertEquals(LockStateEnums.YES_CHANGE, receivedState.get(), "消费者应该接收到正确的锁状态");
    }

    @Test
    @DisplayName("测试 runIfLocked 方法 - 获取锁失败")
    void testRunIfLockedFailure() throws Exception {
        repository.setOwner(testKey, "another-owner");                           // 其他用户占用锁
        repository.setExpireTime(testKey, new Date(System.currentTimeMillis() + 5000)); // 锁未过期
        AtomicBoolean executed = new AtomicBoolean(false);

        ThrowableConsumer<LockStateEnums, Exception> consumer = state -> {
            executed.set(true);
        };

        standardLock.runIfLocked(testOwner, consumer, testTimeout);

        assertFalse(executed.get(), "消费者不应该被执行");
    }

    @Test
    @DisplayName("测试 runIfLocked 方法 - 消费者抛出异常")
    void testRunIfLockedWithException() throws Exception {
        RuntimeException expectedException = new RuntimeException("consumer exception");

        ThrowableConsumer<LockStateEnums, RuntimeException> consumer = state -> {
            throw expectedException;
        };

        RuntimeException thrownException = assertThrows(RuntimeException.class, () -> {
            standardLock.runIfLocked(testOwner, consumer, testTimeout);
        });

        assertEquals(expectedException, thrownException, "应该抛出消费者中的异常");
    }

    @Test
    @DisplayName("测试多次调用 acquire 和 release")
    void testMultipleAcquireAndRelease() {
        // 第一次获取和释放
        assertNotEquals(LockStateEnums.NO, standardLock.acquire(testOwner, testTimeout));
        assertTrue(standardLock.release(testOwner));

        // 第二次获取和释放
        assertNotEquals(LockStateEnums.NO, standardLock.acquire(testOwner, testTimeout));
        assertTrue(standardLock.release(testOwner));
    }
}
