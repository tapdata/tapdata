package io.tapdata.js.connector.base;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class CacheContext {
    private Map<String, CacheData> data = new HashMap<>();
    private final Object lock = new Object();
    private AtomicBoolean alive = new AtomicBoolean(false);
    protected Queue<String> timeLimitedMayGarbage = new ConcurrentLinkedQueue<>();
    private static final long POLL_NANO_TIME = 1000000000L;
    private static final long PERMANENT = 365 * 24 * 60 * 60 * 1000000000L;
    private Thread scavenger;
    private AtomicBoolean running = new AtomicBoolean(true);

    public CacheContext activate(AtomicBoolean alive) {
        if (!this.alive.get()) {
            this.alive = alive;
            this.scavenger = new Thread(() -> {
                while (this.alive.get()) {
                    long start = System.nanoTime();
                    long peekTime = 0L;
                    while (this.running.get() && !timeLimitedMayGarbage.isEmpty() && ((peekTime = System.nanoTime()) + CacheContext.POLL_NANO_TIME) < start) {
                        synchronized (this.lock) {
                            String key = timeLimitedMayGarbage.poll();
                            this.remove(this.data.get(key), key, peekTime);
                        }
                    }
                    try {
                        this.alive.wait(3000);
                    } catch (InterruptedException ignore) {
                    }
                }
            }, "Scavenger");
        }
        return this;
    }

    public void clean() {
        this.alive.notify();
        this.running.set(false);
        if (this.scavenger.isAlive()) {
            this.scavenger.stop();
        }
        this.data = null;
        this.timeLimitedMayGarbage = null;
    }

    public Object get(String key) {
        return this.remove(data.get(key), key, System.nanoTime());
    }

    public boolean release(String key) {
        synchronized (this.lock) {
            if (this.data.containsKey(key)) {
                this.data.remove(key);
                return true;
            }
        }
        return false;
    }

    public Object save(String key, Object data, long lifeCycle) {
        CacheData cacheData = new CacheData();
        cacheData.lifeCycle = lifeCycle;
        cacheData.data = data;
        //cacheData.key = key;
        cacheData.saveTime = System.nanoTime();
        this.data.put(key, cacheData);
        this.timeLimitedMayGarbage.add(key);
        return data;
    }

    public Object save(String key, Object data) {
        return this.save(key, data, CacheContext.PERMANENT);
    }

    private Object remove(CacheData cacheData, String key, long time) {
        if (Objects.nonNull(cacheData)) {
            Object data = cacheData.data;
            if (cacheData.saveTime + cacheData.lifeCycle * 1000000000L < time) {
                this.release(key);
                return null;
            }
            return data;
        }
        return null;
    }

    private class CacheData {
        private long saveTime;
        private long lifeCycle;
        private String key;
        private Object data;
    }
}
