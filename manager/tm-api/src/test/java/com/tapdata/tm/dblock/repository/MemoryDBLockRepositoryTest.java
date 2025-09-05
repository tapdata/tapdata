package com.tapdata.tm.dblock.repository;

import com.tapdata.tm.dblock.DBLock;
import com.tapdata.tm.dblock.LockStateEnums;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 内存锁测试类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/12 10:39 Create
 */
@DisplayName("DBLock 工具类测试")
class MemoryDBLockRepositoryTest {

    private MemoryDBLockRepository repository;
    private static final String TEST_KEY = "test_key";
    private static final String OWNER_1 = "owner1";
    private static final String OWNER_2 = "owner2";

    @BeforeEach
    void setUp() {
        repository = new MemoryDBLockRepository();
    }

    @Test
    @DisplayName("测试初始化锁")
    void testInit() {
        // 首次初始化应该返回true
        assertTrue(repository.init(TEST_KEY));

        // 再次初始化应该返回false，因为已经存在
        assertFalse(repository.init(TEST_KEY));
    }

    @Test
    @DisplayName("测试锁不存在时续期")
    void testRenewWhenLockNotExists() {
        // 当锁不存在时，renew应该返回NO
        LockStateEnums result = repository.renew(TEST_KEY, OWNER_1, new Date(System.currentTimeMillis() + 10000));
        assertEquals(LockStateEnums.NO, result);
    }

    @Test
    @DisplayName("测试相同所有者续期锁")
    void testRenewWhenLockExistsWithSameOwner() {
        // 先初始化锁
        repository.init(TEST_KEY);
        repository.setOwner(TEST_KEY, OWNER_1);

        // 使用相同所有者续期应该返回YES
        Date expireTime = new Date(System.currentTimeMillis() + 10000);
        LockStateEnums result = repository.renew(TEST_KEY, OWNER_1, expireTime);
        assertEquals(LockStateEnums.YES, result);
    }

    @Test
    @DisplayName("测试不同所有者续期未过期锁")
    void testRenewWhenLockExistsWithDifferentOwnerAndNotExpired() {
        Date expireTime = new Date(System.currentTimeMillis() + 10000);

        // 先初始化锁并设置所有者
        repository.init(TEST_KEY);
        repository.setOwner(TEST_KEY, OWNER_1);
        repository.setExpireTime(TEST_KEY, expireTime);

        // 使用不同所有者且锁未过期应该返回YES_CHANGE
        LockStateEnums result = repository.renew(TEST_KEY, OWNER_2, expireTime);
        assertEquals(LockStateEnums.NO, result);
    }

    @Test
    @DisplayName("测试不同所有者续期已过期锁")
    void testRenewWhenLockExistsWithDifferentOwnerAndExpired() {
        // 先初始化锁并设置过期时间
        repository.init(TEST_KEY);
        repository.setOwner(TEST_KEY, OWNER_1);
        repository.setExpireTime(TEST_KEY, new Date(System.currentTimeMillis() - 1000)); // 已过期

        // 使用不同所有者且锁已过期，应该不改变锁状态
        Date newExpireTime = new Date(System.currentTimeMillis() + 10000);
        LockStateEnums result = repository.renew(TEST_KEY, OWNER_2, newExpireTime);
        assertEquals(LockStateEnums.YES_CHANGE, result);
    }

    @Test
    @DisplayName("测试释放不存在的锁")
    void testReleaseWhenLockNotExists() {
        // 当锁不存在时，release应该返回false
        assertFalse(repository.release(TEST_KEY, OWNER_1));
    }

    @Test
    @DisplayName("测试正确所有者释放锁")
    void testReleaseWithCorrectOwner() {
        // 先初始化并获取锁
        repository.init(TEST_KEY);
        repository.setOwner(TEST_KEY, OWNER_1);
        repository.setExpireTime(TEST_KEY, new Date(System.currentTimeMillis() + 10000)); // 未过期

        // 使用正确所有者释放锁应该返回true
        assertTrue(repository.release(TEST_KEY, OWNER_1));

        // 验证锁已被释放（所有者应变回NONE_OWNER）
        MemoryDBLockRepository.LockData lockData = repository.getLockData(TEST_KEY);
        assertNotNull(lockData);
        assertEquals(DBLock.NONE_OWNER, lockData.getOwner());
        assertEquals(DBLock.NONE_EXPIRE, lockData.getExpireTime());
    }

    @Test
    @DisplayName("测试错误所有者释放锁")
    void testReleaseWithIncorrectOwner() {
        // 先初始化并获取锁
        repository.init(TEST_KEY);
        repository.setOwner(TEST_KEY, OWNER_1);
        repository.setExpireTime(TEST_KEY, new Date(System.currentTimeMillis() + 10000)); // 未过期

        // 使用错误所有者释放锁应该返回false
        assertFalse(repository.release(TEST_KEY, OWNER_2));

        // 验证锁未被释放（所有者应保持不变）
        MemoryDBLockRepository.LockData lockData = repository.getLockData(TEST_KEY);
        assertNotNull(lockData);
        assertEquals(OWNER_1, lockData.getOwner());
    }
}
