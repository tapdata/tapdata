package io.tapdata.flow.engine.V2.entity;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.MapUtil;
import com.tapdata.tm.commons.dag.Node;
import io.tapdata.flow.engine.V2.util.StateMapUtil;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-04-01 21:28
 **/
public class PdkStateMapEx extends PdkStateMap {
	public PdkStateMapEx(String nodeId, HazelcastInstance hazelcastInstance) {
		super(nodeId, hazelcastInstance);
	}

	public PdkStateMapEx(HazelcastInstance hazelcastInstance, Node<?> node) {
		super(hazelcastInstance, node);
	}

	@Override
	public Object get(String key) {
		Object value = super.get(key);
		if (value instanceof Map) {
			MapUtil.iterate((Map) value, StateMapUtil::decodeDotAndDollar);
		}
		return value;
	}

	@Override
	public void put(String key, Object o) {
		if (o instanceof Map) {
			MapUtil.iterate((Map) o, k -> {
				if (k.contains(".") || k.contains("$")) {
					return StateMapUtil.encodeDotAndDollar(k);
				}
				return k;
			});
		}
		super.put(key, o);
	}

	@Override
	public Object putIfAbsent(String key, Object o) {
		if (o instanceof Map) {
			MapUtil.iterate((Map) o, k -> {
				if (k.contains(".") || k.contains("$")) {
					return StateMapUtil.encodeDotAndDollar(k);
				}
				return k;
			});
		}
		return super.putIfAbsent(key, o);
	}
}
