package io.tapdata.pdk.apis.entity.message;

import io.tapdata.entity.utils.DataMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class EngineMessage {
	protected String id;

	protected Set<String> subscribeIds;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public abstract String key();

	public Set<String> getSubscribeIds() {
		return subscribeIds;
	}

	public void setSubscribeIds(Set<String> subscribeIds) {
		this.subscribeIds = subscribeIds;
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
}
