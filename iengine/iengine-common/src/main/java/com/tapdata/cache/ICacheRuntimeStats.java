package com.tapdata.cache;

import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.dataflow.StageRuntimeStats;

import java.util.List;

/**
 * 缓存运行状态
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/10/19 下午2:17 Create
 */
public interface ICacheRuntimeStats {

	/**
	 * 设置任务运行状态
	 *
	 * @param dataFlowId                任务编号
	 * @param dataFlowStages            阶段信息
	 * @param dataFlowStageRuntimeStats 运行状态
	 */
	default void setCacheStageRuntimeStats(String dataFlowId, List<Stage> dataFlowStages, List<StageRuntimeStats> dataFlowStageRuntimeStats) {

	}

	/**
	 * 设置任务运行状态
	 *
	 * @param dataFlowId             任务编号
	 * @param cacheName              缓存名
	 * @param cacheStageRuntimeStats 运行状态
	 */
	default void setCacheStageRuntimeStats(String dataFlowId, String cacheName, List<StageRuntimeStats> cacheStageRuntimeStats) {

	}

	/**
	 * 删除任务缓存状态
	 *
	 * @param dataFlowId 任务编号
	 * @param cacheName  缓存名
	 */
	default void removeCacheStageRuntimeStats(String dataFlowId, String cacheName) {

	}


	/**
	 * 获取运行状态
	 *
	 * @param dataFlowId 任务编号
	 * @param cacheName  缓存名
	 * @return 运行状态
	 */
	default List<StageRuntimeStats> getCacheStageRuntimeStats(String dataFlowId, String cacheName) {
		return null;
	}

}
