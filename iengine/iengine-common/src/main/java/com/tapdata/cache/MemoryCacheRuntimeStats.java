package com.tapdata.cache;

import com.tapdata.constant.DataFlowStageUtil;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.dataflow.StageRuntimeStats;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 缓存运行状态
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/10/19 下午2:17 Create
 */
public class MemoryCacheRuntimeStats implements ICacheRuntimeStats {
	private final Map<String, List<StageRuntimeStats>> cacheStageRuntimeStats;
	private final Map<String, String> cacheDataFlowId;

	public MemoryCacheRuntimeStats() {
		cacheStageRuntimeStats = new ConcurrentHashMap<>();
		cacheDataFlowId = new ConcurrentHashMap<>();
	}

	@Override
	public void setCacheStageRuntimeStats(String dataFlowId, List<Stage> dataFlowStages, List<StageRuntimeStats> dataFlowStageRuntimeStats) {
		Map<String, List<StageRuntimeStats>> stageRuntimeStats = new HashMap<>();
		for (StageRuntimeStats stats : dataFlowStageRuntimeStats) {

			String stageId = stats.getStageId();
			Stage targetStage = DataFlowStageUtil.findTargetStageByStageId(stageId, dataFlowStages);
			if (targetStage != null && !targetStage.getDisabled() && Stage.StageTypeEnum.MEM_CACHE.type.equals(targetStage.getType())) {
				if (!stageRuntimeStats.containsKey(targetStage.getCacheName())) {
					stageRuntimeStats.put(targetStage.getCacheName(), new ArrayList<>());
				}
				stageRuntimeStats.get(targetStage.getCacheName()).add(stats);
			}
		}

		if (MapUtils.isNotEmpty(stageRuntimeStats)) {
			for (Map.Entry<String, List<StageRuntimeStats>> entry : stageRuntimeStats.entrySet()) {
				setCacheStageRuntimeStats(
						dataFlowId,
						entry.getKey(),
						entry.getValue()
				);
			}
		}
	}

	@Override
	public synchronized void setCacheStageRuntimeStats(String dataFlowId, String cacheName, List<StageRuntimeStats> cacheStageRuntimeStats) {
		if (null == dataFlowId) {
			throw new RuntimeException("Set the CacheStageRuntimeStats fail because 'dataFlowId' is null");
		} else if (null == cacheName) {
			throw new RuntimeException("Set the CacheStageRuntimeStats fail because 'caseName' is null");
		} else if (this.cacheStageRuntimeStats.containsKey(cacheName)) {
			throw new RuntimeException(String.format("Set the CacheStageRuntimeStats fail because Cache name '%s' already exists, dataFlowId: %s", cacheName, dataFlowId));
		}
		this.cacheStageRuntimeStats.put(cacheName, cacheStageRuntimeStats);
		this.cacheDataFlowId.put(cacheName, dataFlowId);
	}

	@Override
	public synchronized void removeCacheStageRuntimeStats(String dataFlowId, String cacheName) {
		if (null == cacheName) {
			if (null == dataFlowId) {
				throw new RuntimeException("Remove the CacheStageRuntimeStats fail because 'dataFlowId' and 'cacheName' is null");
			}
			List<String> removeNames = new ArrayList<>();
			for (Map.Entry<String, String> en : cacheDataFlowId.entrySet()) {
				if (dataFlowId.equals(en.getValue())) {
					removeNames.add(en.getKey());
					cacheStageRuntimeStats.remove(en.getKey());
				}
			}
			for (String name : removeNames) {
				cacheDataFlowId.remove(name);
			}
		} else {
			cacheDataFlowId.remove(cacheName);
			cacheStageRuntimeStats.remove(cacheName);
		}
	}

	@Override
	public synchronized List<StageRuntimeStats> getCacheStageRuntimeStats(String dataFlowId, String cacheName) {
		if (null == dataFlowId || !dataFlowId.equals(cacheDataFlowId.get(cacheName))) {
			return null;
		}
		return cacheStageRuntimeStats.get(cacheName);
	}
}
