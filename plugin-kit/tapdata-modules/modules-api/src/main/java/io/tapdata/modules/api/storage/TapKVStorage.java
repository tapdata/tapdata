package io.tapdata.modules.api.storage;

import java.util.function.BiFunction;

/**
 * @author aplomb
 */
public interface TapKVStorage extends TapStorage {
	void put(Object key, Object value);
	Object get(Object key);
	Object removeAndGet(Object key);

	void remove(Object key);

	void foreach(BiFunction<Object, Object, Boolean> iterateFunc);

	void foreach(BiFunction<Object, Object, Boolean> iterateFunc, boolean asc);
}
