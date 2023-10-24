package io.tapdata.entity.utils.cache;

public interface Entry<V> {
	String getKey();
	V getValue();
}