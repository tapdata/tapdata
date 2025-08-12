package com.tapdata.tm.dblock.repository;

import com.tapdata.tm.dblock.DBLock;
import com.tapdata.tm.dblock.DBLockRepository;
import com.tapdata.tm.dblock.LockStateEnums;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 内存数据库锁-持久化实现
 * <br/>
 * 用于单测
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/12 09:54 Create
 */
public class MemoryDBLockRepository implements DBLockRepository {

    private final Map<String, LockData> locks = new ConcurrentHashMap<>();

    @Override
    public boolean init(String key) {
        AtomicBoolean init = new AtomicBoolean(false);
        locks.compute(key, (k, v) -> {
            if (null == v) {
                init.set(true);
                return new LockData(key, DBLock.NONE_OWNER, DBLock.NONE_EXPIRE);
            }
            return v;
        });
        return init.get();
    }

    @Override
    public LockStateEnums renew(String key, String owner, Date expireTime) {
        AtomicReference<LockStateEnums> state = new AtomicReference<>(LockStateEnums.NO);
        locks.computeIfPresent(key, (k, v) -> {
            Date currentDate = new Date();
            if (v.owner.equals(owner)) {
                state.set(LockStateEnums.YES);
            } else if (v.expireTime.before(currentDate)) {
                state.set(LockStateEnums.YES_CHANGE);
            } else {
                return v;
            }

            v.setUpdated(currentDate);
            v.setExpireTime(expireTime);
            v.setOwner(owner);
            return v;
        });
        return state.get();
    }

    @Override
    public boolean release(String key, String owner) {
        AtomicBoolean init = new AtomicBoolean(false);
        locks.computeIfPresent(key, (k, v) -> {
            if (v.owner.equals(owner)) {
                init.set(true);
                v.setUpdated(new Date());
                v.setOwner(DBLock.NONE_OWNER);
                v.setExpireTime(DBLock.NONE_EXPIRE);
            }
            return v;
        });
        return init.get();
    }

    public LockData getLockData(String key) {
        return locks.get(key);
    }

    public void setOwner(String key, String owner) {
        locks.computeIfPresent(key, (k, v) -> {
            v.setOwner(owner);
            return v;
        });
    }

    public void setExpireTime(String key, Date expireTime) {
        locks.computeIfPresent(key, (k, v) -> {
            v.setExpireTime(expireTime);
            return v;
        });
    }

    @Getter
    @Setter
    public static class LockData implements Serializable {
        public String key;
        public String owner;
        public Date updated;
        public Date expireTime;

        public LockData(String key, String owner, Date expireTime) {
            this.key = key;
            this.owner = owner;
            this.expireTime = expireTime;
            this.updated = new Date();
        }
    }
}
