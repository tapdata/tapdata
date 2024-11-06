package io.tapdata.flow.engine.V2.task.preview;

import io.tapdata.flow.engine.V2.entity.PdkStateMap;

import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-09-30 18:55
 **/
public class PreviewPdkStateMap extends PdkStateMap {
	private Map<String, Object> map = new HashMap<>();

	public PreviewPdkStateMap() {
	}

	@Override
	public void put(String key, Object o) {
		this.map.put(key, o);
	}

	@Override
	public Object putIfAbsent(String key, Object o) {
		return this.map.putIfAbsent(key, o);
	}

	@Override
	public Object remove(String key) {
		return this.map.remove(key);
	}

	@Override
	public void clear() {
		this.map.clear();
	}

	@Override
	public void reset() {
		this.map = new HashMap<>();
	}

	@Override
	public Object get(String key) {
		return this.map.get(key);
	}
}