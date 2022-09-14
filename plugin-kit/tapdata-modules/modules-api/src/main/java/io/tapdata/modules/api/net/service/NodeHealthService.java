package io.tapdata.modules.api.net.service;

import io.tapdata.modules.api.net.entity.NodeHealth;

import java.util.Collection;

public interface NodeHealthService {
	void save(NodeHealth nodeHealth);
	void delete(String id);
	NodeHealth get(String id);
	Collection<NodeHealth> getHealthNodes();

	String getCleaner();

	boolean applyToBeCleaner(String oldCleaner, String newCleaner);
}
