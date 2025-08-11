package com.tapdata.tm.dblock.impl;

import com.tapdata.tm.dblock.DBLock;
import com.tapdata.tm.dblock.DBLockRepository;
import com.tapdata.tm.dblock.ILock;
import com.tapdata.tm.dblock.LockStateEnums;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;

/**
 * 数据库锁-标准操作实现
 * <p>
 * 提供基于数据库的分布式锁的标准操作实现，包括获取锁、释放锁以及带锁执行任务等功能。
 * 该类封装了锁的基本操作，为上层应用提供简洁的锁操作接口。
 * </p>
 *
 * @param repository 锁数据存储仓库，用于与数据库交互
 * @param key        锁的唯一标识键
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/8 17:50 Create
 */
@Slf4j
public record StandardLock(@Getter DBLockRepository repository, @Getter String key) implements ILock {
    /**
     * 构造函数，初始化标准数据库锁
     * <p>
     * 在构造时会尝试初始化锁键，如果锁键已存在则跳过初始化
     * </p>
     *
     * @param repository 锁数据存储仓库
     * @param key        锁的唯一标识键
     */
    public StandardLock(DBLockRepository repository, String key) {
        this.repository = repository;
        this.key = key;
        if (repository.init(key)) {
            log.debug(DBLock.prefixTag(" lock key '%s' initialized", key));
        } else {
            log.debug(DBLock.prefixTag(" lock key '%s' exists and skip initialization", key));
        }
    }

    /**
     * 获取(续约)锁
     * <p>
     * 尝试获取指定key的锁或续约现有锁，设置锁的过期时间
     * </p>
     *
     * @param owner   锁的所有者标识
     * @param timeout 锁的超时时间(毫秒)
     * @return 锁状态枚举，表示获取锁的结果
     */
    @Override
    public LockStateEnums acquire(String owner, long timeout) {
        Date expireTime = new Date(System.currentTimeMillis() + timeout);
        return repository().renew(key(), owner, expireTime);
    }

    /**
     * 释放锁
     * <p>
     * 释放指定所有者的锁，只有锁的持有者才能成功释放锁
     * </p>
     *
     * @param owner 锁的所有者标识
     * @return 是否成功释放锁
     */
    @Override
    public boolean release(String owner) {
        return repository().release(key(), owner);
    }

}
