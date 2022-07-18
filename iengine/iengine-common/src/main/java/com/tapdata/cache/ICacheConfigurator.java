package com.tapdata.cache;

import com.tapdata.entity.dataflow.DataFlowCacheConfig;

/**
 * 缓存配置接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/9/25 下午3:51 Create
 */
public interface ICacheConfigurator extends ICacheRuntimeStats {

	/**
	 * 注册缓存
	 *
	 * @param cacheConfig 缓存配置
	 */
	void registerCache(DataFlowCacheConfig cacheConfig);

	/**
	 * 根据名称销毁缓存
	 *
	 * @param cacheName 缓存名
	 */
	void destroy(String cacheName);

	/**
	 * 根据缓存名称获取配置
	 *
	 * @param cacheName 缓存名
	 * @return 缓存配置
	 */
	DataFlowCacheConfig getConfig(String cacheName);
}
