package io.tapdata.modules.api.storage;

/**
 * @author aplomb
 */
public interface TapKVStorage extends TapStorage {
	void put(Object key, Object value);
	Object get(Object key);
	void remove(Object key);
}
