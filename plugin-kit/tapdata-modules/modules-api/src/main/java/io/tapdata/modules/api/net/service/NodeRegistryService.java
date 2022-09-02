package io.tapdata.modules.api.net.service;

import io.tapdata.modules.api.net.entity.NodeRegistry;

public interface NodeRegistryService {
	void save(NodeRegistry nodeRegistry);
	void delete(String id);
	NodeRegistry get(String id);
}
