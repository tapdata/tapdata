package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Bean
public class SubscribeMap {
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
}
