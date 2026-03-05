package io.tapdata.threadgroup.utils;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/20 09:43 Create
 * @description
 */
public class FixedConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {
    final long maxSize;
    long size;

    public FixedConcurrentHashMap(long maxSize) {
        super();
        this.maxSize = maxSize;
    }

    @Override
    public V put(K key, V value) {
        if (this.size >= maxSize) {
            return null;
        }
        this.size++;
        return super.put(key, value);
    }

    @Override
    public V remove(Object key) {
        this.size--;
        return super.remove(key);
    }
}
