package io.tapdata.modules.api.net.service;

import io.tapdata.modules.api.net.entity.NodeRegistry;

import java.util.List;

public interface NodeRegistryService {
	void save(NodeRegistry nodeRegistry);
	boolean delete(String id);

	boolean delete(String id, Long time);

	NodeRegistry get(String id);

	List<String> getNodeIds();

	List<NodeRegistry> getNodes();
}
