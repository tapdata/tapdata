package io.tapdata.modules.api.net.service;

import java.util.List;

public interface NodePresenceService {
	void register(List<String> targetIds, String nodeId);
	void unregister(List<String> targetIds, String nodeId);
}
