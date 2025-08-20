package com.tapdata.tm.dblock.impl;

import com.tapdata.tm.commons.function.ThrowableConsumer;
import com.tapdata.tm.commons.util.ThrowableUtils;
import com.tapdata.tm.dblock.DBLock;
import com.tapdata.tm.dblock.ILock;
import com.tapdata.tm.dblock.LockStateEnums;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ActiveServiceLock 类用于实现服务的主备切换机制。
 * 它通过定期心跳检测来维持一个服务实例的活跃状态，并根据锁的状态决定该实例是处于主服务（active）还是备用服务（standby）。
 */
@Slf4j
public class ActiveServiceLock implements AutoCloseable {

    /**
     * 当前服务实例的标识符（通常是唯一ID）
     */
    private final String owner;

    /**
     * 实现具体加锁逻辑的接口实例
     */
    private final ILock lock;

    /**
     * 锁的过期时间（毫秒）
     */
    private final long expireTime;

    /**
     * 当前锁状态的原子引用，用于线程安全地更新锁状态
     */
    private final AtomicReference<LockStateEnums> state;

    /**
     * 标记当前服务实例是否处于活跃状态（主服务）
     */
    private final AtomicBoolean active;

    /**
     * 心跳任务的调度句柄，用于取消定时任务
     */
    private final ScheduledFuture<?> heartbeatFuture;

    /**
     * 当服务变为活跃状态时执行的操作
     */
    private final ThrowableConsumer<ActiveServiceLock, Exception> doActive;

    /**
     * 当服务变为备用状态时执行的操作
     */
    private final ThrowableConsumer<ActiveServiceLock, Exception> doStandby;

    /**
     * 构造函数初始化 ActiveServiceLock 实例
     *
     * @param lock             实现锁逻辑的接口实例
     * @param owner            服务实例标识符
     * @param expireSeconds    锁的过期时间（秒）
     * @param heartbeatSeconds 心跳间隔时间（秒）
     * @param doActive         转为主服务时执行的操作
     * @param doStandby        转为备用服务时执行的操作
     */
    public ActiveServiceLock(ILock lock, String owner, long expireSeconds, long heartbeatSeconds
        , ThrowableConsumer<ActiveServiceLock, Exception> doActive
        , ThrowableConsumer<ActiveServiceLock, Exception> doStandby
    ) {
        this.lock = lock;
        this.owner = owner;
        this.expireTime = TimeUnit.SECONDS.toMillis(expireSeconds);
        this.state = new AtomicReference<>();
        this.active = new AtomicBoolean(false);
        // 启动定时心跳任务
        this.heartbeatFuture = DBLock.executor.scheduleWithFixedDelay(this::doHeartbeat, 5, heartbeatSeconds, TimeUnit.SECONDS);
        this.doActive = doActive;
        this.doStandby = doStandby;
    }

    /**
     * 执行一次心跳操作，尝试获取锁并根据锁状态更新服务状态
     */
    protected void doHeartbeat() {
        try {
            // 尝试获取锁并更新锁状态
            state.set(lock.acquire(owner, expireTime));
            switch (state.get()) {
                case NO: {
                    // 如果当前是活跃状态但未能获得锁，则切换到备用状态
                    if (isActive()) {
                        active.set(false);
                        doStandby.accept(this);
                    }
                    break;
                }
                case YES:
                case YES_CHANGE: {
                    // 如果当前不是活跃状态但成功获得锁，则切换到活跃状态
                    if (!isActive()) {
                        active.set(true);
                        doActive.accept(this);
                    }
                    break;
                }
                default:
                    log.warn(DBLock.prefixTag(" unsupported state '%s' for '%s'", state.get(), lock.getKey()));
                    break;
            }
        } catch (Exception e) {
            log.error(DBLock.prefixTag(" heartbeat failed for '%s'", lock.getKey()), e);
        }
    }

    /**
     * 判断当前服务实例是否处于活跃状态
     *
     * @return true 表示当前是活跃状态（主服务），false 表示备用状态
     */
    public boolean isActive() {
        return active.get();
    }

    /**
     * 获取锁的键值
     *
     * @return 返回锁对象关联的键值字符串
     */
    public String getKey() {
        return lock.getKey();
    }


    /**
     * 关闭 ActiveServiceLock 实例，取消心跳任务
     *
     * @throws Exception 如果在关闭过程中发生异常
     */
    @Override
    public void close() throws Exception {
        ThrowableUtils.collector(e -> {
                if (e instanceof Exception ex) {
                    return ex;
                } else {
                    return new Exception(e);
                }
            })
            .collect(() -> heartbeatFuture.cancel(true)) // 取消心跳任务
            .throwIfPresent(); // 如果有异常则抛出
    }
}
