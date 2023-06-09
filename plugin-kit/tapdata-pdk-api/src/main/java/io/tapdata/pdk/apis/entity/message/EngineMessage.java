package io.tapdata.pdk.apis.entity.message;

import io.tapdata.entity.tracker.MessageTracker;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class EngineMessage extends MessageTracker {
	protected Integer timeoutSeconds;
	protected String id;

	protected Set<String> subscribeIds;
	protected List<Set<String>> orSubscribeIds;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public abstract String key();

	public List<Set<String>> getOrSubscribeIds() {
		return orSubscribeIds;
	}

	public void setOrSubscribeIds(List<Set<String>> orSubscribeIds) {
		this.orSubscribeIds = orSubscribeIds;
	}

	public Set<String> getSubscribeIds() {
		return subscribeIds;
	}

	public void setSubscribeIds(Set<String> subscribeIds) {
		this.subscribeIds = subscribeIds;
	}

	public Integer getTimeoutSeconds() {
		return timeoutSeconds;
	}

	public void setTimeoutSeconds(Integer timeoutSeconds) {
		this.timeoutSeconds = timeoutSeconds;
	}

	public void subscribeIds(String... subscribeIds) {
		if(this.subscribeIds == null)
			this.subscribeIds = new HashSet<>();
		if(subscribeIds != null) {
			for(String subscribeId : subscribeIds) {
				if(!this.subscribeIds.contains(subscribeId))
					this.subscribeIds.add(subscribeId);
			}
		}
	}

	public void orSubscribeIdSets(Set<String>... subscribeIdSets) {
		if(this.orSubscribeIds == null)
			this.orSubscribeIds = new ArrayList<>();
		if(subscribeIdSets != null) {
			for(Set<String> subscribeIdSet : subscribeIdSets) {
				if(subscribeIdSet != null && !subscribeIdSet.isEmpty()) {
					this.orSubscribeIds.add(subscribeIdSet);
				}
			}
		}
	}
}
