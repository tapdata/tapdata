package io.tapdata.modules.api.storage;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author aplomb
 */
public interface TapKVStorage extends TapStorage {
	void put(Object key, Object value);
	Object get(Object key);
	Object removeAndGet(Object key);

	void remove(Object key);
	interface GetObject {
		Object get(Object key);
	}
	void foreachValues(BiFunction<Object, Object, Boolean> iterateFunc, GetObject getObject, boolean asc);

	void foreachValues(BiFunction<Object, Object, Boolean> iterateFunc, GetObject getObject);

	void foreachValues(Function<Object, Boolean> iterateFunc);
	void foreachValues(Function<Object, Boolean> iterateFunc, boolean asc);

	void foreach(BiFunction<Object, Object, Boolean> iterateFunc);

	void foreach(BiFunction<Object, Object, Boolean> iterateFunc, boolean asc);
}
