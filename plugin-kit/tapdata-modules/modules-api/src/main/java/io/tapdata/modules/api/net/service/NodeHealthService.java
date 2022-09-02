package io.tapdata.modules.api.net.service;

import io.tapdata.modules.api.net.entity.NodeHealth;
import io.tapdata.modules.api.net.entity.NodeRegistry;

import java.util.Collection;
import java.util.Map;

public interface NodeHealthService {
	void save(NodeHealth nodeHealth);
	void delete(String id);
	NodeHealth get(String id);
	Collection<NodeHealth> getHealthNodes();
}
