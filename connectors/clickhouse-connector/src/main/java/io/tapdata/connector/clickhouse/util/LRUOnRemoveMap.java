package io.tapdata.connector.clickhouse.util;

import org.apache.commons.collections4.map.LRUMap;

import java.util.function.Consumer;

/**
 * @author dayun
 * @date 2022/6/24 16:47
 */
public class LRUOnRemoveMap<K, V> extends LRUMap<K, V> {
    private final Consumer<Entry<K, V>> onRemove;

    public LRUOnRemoveMap(int maxSize, Consumer<Entry<K, V>> onRemove) {
        super(maxSize);
        this.onRemove = onRemove;
    }

    @Override
    public void clear() {
        LinkEntry<K, V> entry;
        for (K key : keySet()) {
            entry = getEntry(key);
            onRemove.accept(entry);
            removeLRU(entry);
        }
    }

    @Override
    protected void removeEntry(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
        onRemove.accept(entry);
        super.removeEntry(entry, hashIndex, previous);
    }

}
