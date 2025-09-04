package com.tapdata.tm.dblock;

import com.tapdata.tm.commons.function.ThrowableConsumer;
import com.tapdata.tm.commons.function.ThrowableFunction;
import com.tapdata.tm.dblock.impl.ActiveServiceLock;
import org.springframework.util.Assert;

/**
 * 数据库锁接口，定义了分布式锁的基本操作和高级功能。
 * <p>
 * 该接口提供了获取、释放锁的基础方法，以及基于锁状态执行操作的便捷方法。
 * 同时支持创建用于主备切换的服务锁实例。
 * <p>
 * 锁的状态由 {@link LockStateEnums} 枚举表示，包括：
 * - NO: 未能获取锁
 * - YES: 成功获取锁
 * - YES_CHANGE: 成功获取锁且状态发生了变化
 * </p>
 */
public interface ILock {

    /**
     * 获取锁的仓库
     *
     * @return 锁的仓库，用于与数据库交互进行锁操作
     */
    DBLockRepository getRepository();

    /**
     * 获取锁的key
     *
     * @return 锁的唯一标识符
     */
    String getKey();

    /**
     * 获取(续约)锁
     * <p>
     * 尝试获取指定key的锁或续约现有锁，设置锁的过期时间。
     * 如果当前锁已被其他所有者持有且未过期，则返回 NO。
     * 如果成功获取锁或续约现有锁，则返回 YES 或 YES_CHANGE。
     * </p>
     *
     * @param owner   锁的所有者标识，通常是服务实例的唯一ID
     * @param timeout 锁的超时时间(毫秒)，超过此时间锁将自动失效
     * @return 锁状态枚举，表示获取锁的结果
     * - NO: 未能获取锁
     * - YES: 成功获取锁
     * - YES_CHANGE: 成功获取锁且状态发生了变化
     */
    LockStateEnums acquire(String owner, long timeout);

    /**
     * 释放锁
     * <p>
     * 释放指定所有者的锁，只有锁的持有者才能成功释放锁。
     * 如果锁已过期或被其他所有者持有，则释放失败。
     * </p>
     *
     * @param owner 锁的所有者标识，必须与获取锁时的owner一致
     * @return 是否成功释放锁
     */
    boolean release(String owner);

    /**
     * 如果成功获取锁则执行函数
     * <p>
     * 尝试获取锁，如果成功则执行指定函数，并在执行完成后自动释放锁。
     * 这是一个便捷方法，确保锁的正确获取和释放。
     * </p>
     *
     * @param owner   锁的所有者标识
     * @param fn      要执行的函数，接收锁状态作为参数
     * @param timeout 锁的超时时间(毫秒)
     * @param <R>     函数返回值类型
     * @param <E>     函数可能抛出的异常类型
     * @return 函数执行结果，如果获取锁失败则返回null
     * @throws E 函数执行可能抛出的异常
     */
    default <R, E extends Exception> R callIfLocked(String owner, ThrowableFunction<R, LockStateEnums, E> fn, long timeout) throws E {
        LockStateEnums state = acquire(owner, timeout);
        if (LockStateEnums.NO == state) return null;

        try {
            return fn.apply(state);
        } finally {
            release(owner);
        }
    }

    /**
     * 如果成功获取锁则执行消费者操作
     * <p>
     * 尝试获取锁，如果成功则执行指定的消费者操作，并在执行完成后自动释放锁。
     * 这是一个便捷方法，适用于不需要返回值的场景。
     * </p>
     *
     * @param owner    锁的所有者标识
     * @param consumer 要执行的消费者操作，接收锁状态作为参数
     * @param timeout  锁的超时时间(毫秒)
     * @param <E>      消费者操作可能抛出的异常类型
     * @throws E 消费者操作可能抛出的异常
     */
    default <E extends Exception> void runIfLocked(String owner, ThrowableConsumer<LockStateEnums, E> consumer, long timeout) throws E {
        LockStateEnums state = acquire(owner, timeout);
        if (LockStateEnums.NO == state) return;

        try {
            consumer.accept(state);
        } finally {
            release(owner);
        }
    }

    /**
     * 创建用于主备切换的服务锁实例
     * <p>
     * 创建一个 {@link ActiveServiceLock} 实例，用于实现服务的主备切换机制。
     * 通过定期心跳检测来维持服务实例的活跃状态，并根据锁的状态决定该实例是主服务还是备用服务。
     * </p>
     *
     * @param owner            服务实例标识符
     * @param expireSeconds    锁的过期时间（秒），必须大于心跳间隔时间
     * @param heartbeatSeconds 心跳间隔时间（秒），定期检查锁状态
     * @param doActive         当服务变为活跃状态（主服务）时执行的操作
     * @param doStandby        当服务变为备用状态时执行的操作
     * @return ActiveServiceLock 实例，用于管理服务的主备切换
     * @throws IllegalArgumentException 如果参数不符合要求（如expireSeconds <= heartbeatSeconds）
     */
    default ActiveServiceLock activeService(String owner, long expireSeconds, long heartbeatSeconds
        , ThrowableConsumer<ActiveServiceLock, Exception> doActive, ThrowableConsumer<ActiveServiceLock, Exception> doStandby) {
        Assert.notNull(owner, "owner cannot be null");
        Assert.notNull(doActive, "doActive cannot be null");
        Assert.notNull(doStandby, "doStandby cannot be null");
        // 验证过期时间必须大于心跳间隔时间，确保锁能正常续约
        Assert.isTrue(expireSeconds > heartbeatSeconds,
            String.format("expireSeconds(%s) must be greater than heartbeatSeconds(%s)", expireSeconds, heartbeatSeconds));

        return new ActiveServiceLock(this, owner, expireSeconds, heartbeatSeconds, doActive, doStandby);
    }

}
