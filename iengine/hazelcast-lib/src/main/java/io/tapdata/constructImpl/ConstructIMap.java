package io.tapdata.constructImpl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

/**
 * @author samuel
 * @Description
 * @create 2021-11-23 17:09
 **/
public class ConstructIMap<T> extends BaseConstruct<T> {

	private IMap<String, T> iMap;

	public ConstructIMap(HazelcastInstance hazelcastInstance, String name) {
		this.iMap = hazelcastInstance.getMap(name);
	}

	@Override
	public int insert(String key, T data) throws Exception {
		iMap.put(key, data);
		return 1;
	}

	@Override
	public int update(String key, T data) throws Exception {
		iMap.put(key, data);
		return 1;
	}

	@Override
	public int upsert(String key, T data) throws Exception {
		iMap.put(key, data);
		return 1;
	}

	@Override
	public int delete(String key) throws Exception {
		int delete = 0;

		iMap.remove(key);
		delete++;

		return delete;
	}

	@Override
	public T find(String key) throws Exception {
		return iMap.get(key);
	}

	@Override
	public boolean exists(String key) throws Exception {
		return iMap.containsKey(key);
	}

	@Override
	public void clear() throws Exception {
		iMap.clear();
	}

	@Override
	public boolean isEmpty() {
		if (null == this.iMap) {
			return true;
		}
		return this.iMap.isEmpty();
	}

	@Override
	public String getName() {
		return iMap.getName();
	}

	@Override
	public String getType() {
		return "IMap";
	}
}
