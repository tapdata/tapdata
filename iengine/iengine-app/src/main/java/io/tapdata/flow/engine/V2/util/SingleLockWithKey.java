package io.tapdata.flow.engine.V2.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/9/5 10:22 Create
 */
public class SingleLockWithKey {
    private final Map<String, AtomicBoolean> locks = new HashMap<>();

    private void lock(String key) throws InterruptedException {
        while (!Thread.interrupted()) {
            AtomicBoolean lock;
            synchronized (locks) {
                lock = locks.get(key);
                if (null == lock) {
                    locks.put(key, new AtomicBoolean(true));
                    return;
                }
            }
            synchronized (lock) {
                if (lock.get()) {
                    lock.wait();
                }
            }
        }
    }

    private boolean tryLock(String key, long timeout, TimeUnit timeUnit) throws InterruptedException {
        long timeoutMills = timeUnit.toMillis(timeout);
        long s = System.currentTimeMillis();
        while (!Thread.interrupted()) {
            AtomicBoolean lock;
            synchronized (locks) {
                lock = locks.get(key);
                if (null == lock) {
                    locks.put(key, new AtomicBoolean(true));
                    return true;
                }
            }
            if ((System.currentTimeMillis() - s) > timeoutMills) {
                break;
            }
			TimeUnit.MILLISECONDS.sleep(1L);
        }
        return false;
    }

    private void unlock(String key) {
        AtomicBoolean lock;
        synchronized (locks) {
            lock = locks.remove(key);
			if (null == lock) return;
        }
        synchronized (lock) {
            lock.set(false);
            lock.notifyAll();
        }
    }

    public <T> T call(String key, Callable<T> callable) throws Exception {
        try {
            lock(key);
            return callable.call();
        } finally {
            unlock(key);
        }
    }

    public void run(String key, Runnable runnable) throws InterruptedException {
        try {
            lock(key);
            runnable.run();
        } finally {
            unlock(key);
        }
    }

    public boolean tryRun(String key, Runnable runnable, long timeout, TimeUnit timeUnit) throws InterruptedException {
        if (tryLock(key, timeout, timeUnit)) {
            try {
                runnable.run();
                return true;
            } finally {
                unlock(key);
            }
        }
        return false;
    }

    public Runnable wrap(String key, Runnable runnable) {
        return () -> {
            try {
                lock(key);
                runnable.run();
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            } finally {
                unlock(key);
            }
        };
    }
}
