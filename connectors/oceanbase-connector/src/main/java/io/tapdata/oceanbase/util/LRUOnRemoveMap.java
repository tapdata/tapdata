package io.tapdata.oceanbase.util;

import org.apache.commons.collections4.map.LRUMap;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author dayun
 * @date 2022/6/24 16:47
 */
public class LRUOnRemoveMap<K, V> extends LRUMap<K, V> {
    private Consumer<Entry<K, V>> onRemove;

    public LRUOnRemoveMap(int maxSize, Consumer<Map.Entry<K, V>> onRemove) {
        super(maxSize);
        this.onRemove = onRemove;
    }

    @Override
    protected boolean removeLRU(LinkEntry<K, V> entry) {
        onRemove.accept(entry);
        return super.removeLRU(entry);
    }

    @Override
    public void clear() {
        Set<Entry<K, V>> entries = this.entrySet();
        for (Map.Entry<K, V> entry : entries) {
            onRemove.accept(entry);
        }
        super.clear();
    }

//    @Override
//    private void removeEntry(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
//        onRemove.accept(entry);
//        super.removeEntry(entry, hashIndex, previous);
//    }
//
//    @Override
//    private void removeMapping(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
//        onRemove.accept(entry);
//        super.removeMapping(entry, hashIndex, previous);
//    }
}
