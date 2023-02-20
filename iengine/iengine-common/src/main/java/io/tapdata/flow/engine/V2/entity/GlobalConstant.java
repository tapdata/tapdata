package io.tapdata.flow.engine.V2.entity;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.ConfigurationCenter;

/**
 * @author samuel
 * @Description
 * @create 2022-10-20 14:17
 **/
public class GlobalConstant {
	private ConfigurationCenter configurationCenter;

	public GlobalConstant configurationCenter(ConfigurationCenter configurationCenter) {
		this.configurationCenter = configurationCenter;
		return this;
	}

	public ConfigurationCenter getConfigurationCenter() {
		return configurationCenter;
	}

	private HazelcastInstance hazelcastInstance;

	public GlobalConstant hazelcastInstance(HazelcastInstance hazelcastInstance) {
		this.hazelcastInstance = hazelcastInstance;
		return this;
	}

	private GlobalConstant() {
	}

	public static GlobalConstant getInstance() {
		return GlobalConstantInstance.INSTANCE.globalConstant;
	}

	enum GlobalConstantInstance {
		INSTANCE,
		;
		private final GlobalConstant globalConstant;

		GlobalConstantInstance() {
			this.globalConstant = new GlobalConstant();
		}
	}
}
