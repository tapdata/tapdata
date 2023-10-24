package io.tapdata.mongodb.net;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.modules.api.net.entity.NodeHealth;
import io.tapdata.modules.api.net.service.node.NodeHealthService;
import io.tapdata.mongodb.entity.NodeHealthMapEntity;
import io.tapdata.mongodb.net.dao.NodeHealthDAO;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Implementation(NodeHealthService.class)
public class NodeHealthServiceImpl implements NodeHealthService {
	public static final String ID = "nodes_health";

	public static final int unhealthySeconds = 45;

	@Bean
	private NodeHealthDAO nodeHealthDAO;
	@Override
	public void save(NodeHealth nodeHealth) {
		nodeHealthDAO.addNodeHealth(nodeHealth);
	}

	@Override
	public boolean delete(String id) {
		return nodeHealthDAO.deleteNodeHealth(id);
	}

	@Override
	public NodeHealth get(String id) {
		return nodeHealthDAO.getNodeHealth(id);
	}

	@Override
	public Collection<NodeHealth> getHealthNodes() {
		SortedSet<NodeHealth> nodeHealthSortedSet = Collections.synchronizedSortedSet(new TreeSet<>());
		NodeHealthMapEntity nodeHealthMapEntity = nodeHealthDAO.get();
		if(nodeHealthMapEntity != null && nodeHealthMapEntity.getMap() != null) {
			for(Map.Entry<String, NodeHealth> entry : nodeHealthMapEntity.getMap().entrySet()) {
				String id = entry.getKey();
				NodeHealth nodeHealth = entry.getValue();
				if(nodeHealth.getTime() == null || nodeHealth.getHealth() == null) {
					continue;
				}
				if(nodeHealth.getHealth() >= 0 && System.currentTimeMillis() - nodeHealth.getTime() <= TimeUnit.SECONDS.toMillis(unhealthySeconds)) {
					nodeHealthSortedSet.add(nodeHealth.id(id));
				}
			}
		}
		return nodeHealthSortedSet;
	}

	@Override
	public String getCleaner() {
		return nodeHealthDAO.getCleaner();
	}

	@Override
	public boolean applyToBeCleaner(String oldCleaner, String newCleaner) {
		return nodeHealthDAO.assignCleaner(oldCleaner, newCleaner);
	}
}
