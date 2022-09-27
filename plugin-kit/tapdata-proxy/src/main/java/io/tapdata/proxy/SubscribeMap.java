package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.memory.MemoryFetcher;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Bean
public class SubscribeMap implements MemoryFetcher {
	private final Map<String, List<EngineSessionHandler>> subscribeIdSessionMap = new ConcurrentHashMap<>();

	public Set<String> rebindSubscribeIds(EngineSessionHandler engineSessionHandler, Set<String> newSubscribeIds, Set<String> oldSubscribeIds) {
		if(newSubscribeIds == null && oldSubscribeIds == null)
			return null;
		Set<String> added = null;
		Set<String> deleted = null;
		if(oldSubscribeIds == null || oldSubscribeIds.isEmpty())
			added = newSubscribeIds;
		else if(newSubscribeIds == null || newSubscribeIds.isEmpty())
			deleted = oldSubscribeIds;
		else {
			for(String newSubId : newSubscribeIds) {
				if(!oldSubscribeIds.contains(newSubId)) {
					if(added == null)
						added = new HashSet<>();
					added.add(newSubId);
				}
			}
			for(String oldSubId : oldSubscribeIds) {
				if(!newSubscribeIds.contains(oldSubId)) {
					if(deleted == null)
						deleted = new HashSet<>();
					deleted.add(oldSubId);
				}
			}
		}
		if(added != null) {
			for(String addStr : added) {
				List<EngineSessionHandler> engineSessionHandlers = subscribeIdSessionMap.computeIfAbsent(addStr, s -> new CopyOnWriteArrayList<>());
				if(!engineSessionHandlers.contains(engineSessionHandler)) {
					engineSessionHandlers.add(engineSessionHandler);
				}
			}
		}
		if(deleted != null) {
			for(String deleteStr : deleted) {
				List<EngineSessionHandler> engineSessionHandlers = subscribeIdSessionMap.computeIfAbsent(deleteStr, s -> new CopyOnWriteArrayList<>());
				engineSessionHandlers.remove(engineSessionHandler);
			}
		}

		return newSubscribeIds;
	}

	public Map<String, List<EngineSessionHandler>> getSubscribeIdSessionMap() {
		return subscribeIdSessionMap;
	}

	public Map<EngineSessionHandler, List<String>> getSessionSubscribeIdsMap(Set<String> cachingChangedSubscribeIds) {
		Map<EngineSessionHandler, List<String>> sessionSubscribeIdsMap = new HashMap<>();
		for(String subscribeId : cachingChangedSubscribeIds) {
			List<EngineSessionHandler> engineSessionHandlers = subscribeIdSessionMap.get(subscribeId);
			if(engineSessionHandlers != null) {
				for(EngineSessionHandler engineSessionHandler : engineSessionHandlers) {
					List<String> list = sessionSubscribeIdsMap.get(engineSessionHandler);
					if(list == null)
						list = sessionSubscribeIdsMap.computeIfAbsent(engineSessionHandler, engineSessionHandler1 -> new ArrayList<>());
					if(!list.contains(subscribeId)) {
						list.add(subscribeId);
					}
				}
			}
		}
		return sessionSubscribeIdsMap;
	}

	@Override
	public DataMap memory(List<String> mapKeys, String memoryLevel) {
		DataMap dataMap = DataMap.create();
		for(Map.Entry<String, List<EngineSessionHandler>> entry : subscribeIdSessionMap.entrySet()) {
			List<EngineSessionHandler> handlers = entry.getValue();
			if(handlers != null) {
				List<String> handlerIds = new ArrayList<>();
				for(EngineSessionHandler handler : handlers) {
					handlerIds.add(handler.getId());
				}
				dataMap.put(entry.getKey(), handlerIds);
			}
		}
		return dataMap;
	}
}
