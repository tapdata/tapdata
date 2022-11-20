package io.tapdata.modules.api.async.master;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

/**
 * @author aplomb
 */
public interface AsyncTools {
	<T> void foreach(Collection<T> collection, Function<T, Boolean> function);
	<K, V> void foreach(Map<K, V> map, Function<Map.Entry<K, V>, Boolean> entryFunction);
	void runOnce(Runnable runnable);
}
