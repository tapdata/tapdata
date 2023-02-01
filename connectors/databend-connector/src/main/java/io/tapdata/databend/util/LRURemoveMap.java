package io.tapdata.databend.util;

import org.apache.commons.collections4.map.LRUMap;

import java.util.function.Consumer;


public class LRURemoveMap<K, V> extends LRUMap<K, V> {
    private final Consumer<Entry<K, V>> onRemove;

    public LRURemoveMap(int maxSize, Consumer<Entry<K, V>> onRemove) {
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
