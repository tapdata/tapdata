package io.tapdata.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVMap;

/**
 * @author samuel
 * @Description
 * @create 2022-05-17 00:20
 **/
public class PdkTableMap implements KVMap<TapTable> {
	private TapTableMap<String, TapTable> tapTableMap;

	public PdkTableMap(TapTableMap<String, TapTable> tapTableMap) {
		this.tapTableMap = tapTableMap;
	}

	@Override
	public void init(String mapKey, Class<TapTable> valueClass) {
	}

	@Override
	public void put(String key, TapTable tapTable) {
	}

	@Override
	public TapTable putIfAbsent(String key, TapTable tapTable) {
		return null;
	}

	@Override
	public TapTable remove(String key) {
		return null;
	}

	@Override
	public void clear() {

	}

	@Override
	public void reset() {

	}

	@Override
	public TapTable get(String key) {
		return tapTableMap.get(key);
	}

	@Override
	public Iterator<Entry<TapTable>> iterator() {
		return tapTableMap.iterator();
	}
}
