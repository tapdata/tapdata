package com.tapdata.tm.dblock;

import java.util.Date;

/**
 * 数据库锁持久化操作接口
 * <p>
 * 该接口定义了数据库锁的核心操作方法，包括初始化锁、续期锁和释放锁等功能。
 * 通过这些方法可以实现基于数据库的分布式锁机制，确保在分布式环境下的数据一致性。
 * </p>
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/8 12:23 Create
 */
public interface DBLockRepository {

    /**
     * 初始化指定key的锁
     *
     * @param key 锁的唯一标识
     * @return 初始化成功返回true，否则返回false
     */
    boolean init(String key);

    /**
     * 续期指定key的锁
     *
     * @param key        锁的唯一标识
     * @param owner      锁的持有者标识
     * @param expireTime 锁的过期时间
     * @return 锁状态枚举值
     */
    LockStateEnums renew(String key, String owner, Date expireTime);

    /**
     * 释放指定key的锁
     *
     * @param key   锁的唯一标识
     * @param owner 锁的持有者标识
     * @return 释放成功返回true，否则返回false
     */
    boolean release(String key, String owner);
}
