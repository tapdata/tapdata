package io.tapdata.flow.engine.V2.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/9/5 10:22 Create
 */
public class SingleLockWithKey {
    private final Map<String, AtomicBoolean> locks = new HashMap<>();

    public void lock(String key) throws InterruptedException {
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

    public void unlock(String key) {
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

    public Runnable wrap(String key, Runnable runnable) {
        return () -> {
            try {
                lock(key);
                runnable.run();
            } catch (InterruptedException ignore) {
            } finally {
                unlock(key);
            }
        };
    }

//    public static void main(String[] args) throws Exception {
//        int dataSize = 100000, keySize = 10, threadCounts = 10, interval = 10;
//
//        SingleLockWithKey singleLockWithKey = new SingleLockWithKey();
//        ExecutorService executorService = Executors.newFixedThreadPool(threadCounts);
//
//        CountDownLatch countDownLatch = new CountDownLatch(dataSize);
//        for (int i = 0; i < dataSize; i++) {
//            int finalIndex = i;
//            executorService.execute(() -> {
//                try {
//                    singleLockWithKey.call("task-" + (finalIndex % keySize), () -> {
////                        TimeUnit.MILLISECONDS.sleep(interval);
//                        logger.info("run {} with key: {}", finalIndex, (finalIndex % keySize));
//                        return null;
//                    });
//                } catch (Exception e) {
//                    logger.warn("return with interrupted: {}", finalIndex, e);
//                } finally {
//                    countDownLatch.countDown();
//                }
//            });
//        }
//
//        countDownLatch.await();
//        executorService.shutdown();
//    }
}
