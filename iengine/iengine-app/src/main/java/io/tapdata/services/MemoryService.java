package io.tapdata.services;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.CacheStatistics;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastMergeNode;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.service.skeleton.annotation.RemoteService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RemoteService
public class MemoryService {
	public DataMap memory(List<String> keys, String keyRegex, String memoryLevel) {
		return PDKIntegration.outputMemoryFetchersInDataMap(keys, keyRegex, memoryLevel);
	}

	public DataMap mergeCacheManager(List<String> keys, Map<String,String> info, boolean isRunning, ExternalStorageDto externalStorageDto, String keyRegex, String memoryLevel) {
		DataMap L1 = new DataMap();
		DataMap L2 = new DataMap();

		// Get local cache statistics when running
		if(isRunning){
			 L1 = PDKIntegration.outputMemoryFetchersInDataMap(keys, keyRegex, memoryLevel);
			 L1.remove("removed");
		}

		// Get remote cache statistics
		for(Map.Entry<String,String> entry : info.entrySet()) {
			String cacheName = HazelcastMergeNode.getMergeCacheName(entry.getKey(), entry.getValue());
			try{
				if(!isRunning){
					HazelcastInstance hazelcastInstance = HazelcastUtil.getInstance();
					ExternalStorageUtil.initHZMapStorage(externalStorageDto, HazelcastMergeNode.class.getSimpleName(),String.valueOf(cacheName.hashCode()),hazelcastInstance.getConfig());
				}
				Map<String,Object> statistics = PersistenceStorage.getInstance().getStatistics(ConstructType.IMAP, String.valueOf(cacheName.hashCode()));
				if(null == statistics)continue;
				L2.put(entry.getKey(), CacheStatistics.createRemoteCache((Number) statistics.get("size"),(Number) statistics.get("count"), (String) statistics.get("uri"), (String) statistics.get("mode")));
			}finally {
				if(!isRunning){
					PersistenceStorage.getInstance().destroy(HazelcastMergeNode.class.getSimpleName(), ConstructType.IMAP, String.valueOf(cacheName.hashCode()));
				}
			}
		}

		DataMap result = DataMap.create();
		for(String s : keys) {
			DataMap map = (DataMap) L1.get(s);
			if(map != null) {
				for(Map.Entry<String, Object> entry : map.entrySet()) {
					String key = entry.getKey();
					List<CacheStatistics> cacheStatsList = new ArrayList<>();

					Object localStats = entry.getValue();
					if(localStats instanceof CacheStatistics) {
						cacheStatsList.add((CacheStatistics) localStats);
					}

					Object remoteStats = L2.get(key);
					if(remoteStats instanceof CacheStatistics) {
						cacheStatsList.add((CacheStatistics) remoteStats);
					}
					if(info.containsKey(key)) {
						result.put(key, cacheStatsList);
					}
				}
			}
		}

		for(Map.Entry<String, Object> entry : L2.entrySet()) {
			String key = entry.getKey();
			if(info.containsKey(key) && !result.containsKey(key)){
				List<CacheStatistics> cacheStatsList = new ArrayList<>();
				Object remoteStats = entry.getValue();
				if(remoteStats instanceof CacheStatistics) {
					cacheStatsList.add((CacheStatistics) remoteStats);
				}
				result.put(key, cacheStatsList);
			}
		}

		return result;
	}
}
