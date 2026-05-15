package io.tapdata.threadgroup.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/20 09:43 Create
 * @description A size-limited ConcurrentHashMap that rejects new entries when maxSize is reached.
 */
public class FixedConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {
    final long maxSize;
    final AtomicLong size = new AtomicLong(0);

    public FixedConcurrentHashMap(long maxSize) {
        super();
        this.maxSize = maxSize;
    }

    @Override
    public V put(K key, V value) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        // Check if key already exists (replacement, not new entry)
        boolean isNewKey = !containsKey(key);
        if (isNewKey) {
            // Only check size limit for new keys
            if (size.get() >= maxSize) {
                return null;
            }
        }
        V oldValue = super.put(key, value);
        // Only increment size if this was a new key (oldValue is null)
        if (oldValue == null && isNewKey) {
            size.incrementAndGet();
        }
        return oldValue;
    }

    @Override
    public V remove(Object key) {
        if (key == null) {
            return null;
        }
        V removed = super.remove(key);
        // Only decrement size if we actually removed something
        if (removed != null) {
            size.decrementAndGet();
        }
        return removed;
    }

    /**
     * Returns the current size of the map.
     * @return the current number of entries
     */
    public long getSize() {
        return size.get();
    }
}
