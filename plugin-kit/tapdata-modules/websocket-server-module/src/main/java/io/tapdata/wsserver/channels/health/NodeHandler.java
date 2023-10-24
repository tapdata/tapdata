package io.tapdata.wsserver.channels.health;

import io.tapdata.modules.api.net.entity.NodeHealth;
import io.tapdata.modules.api.net.entity.NodeRegistry;

public class NodeHandler {
	private NodeHealth nodeHealth;
	public NodeHandler nodeHealth(NodeHealth nodeHealth) {
		this.nodeHealth = nodeHealth;
		return this;
	}
	private NodeRegistry nodeRegistry;
	public NodeHandler nodeRegistry(NodeRegistry nodeRegistry) {
		this.nodeRegistry = nodeRegistry;
		return this;
	}

	public NodeHealth getNodeHealth() {
		return nodeHealth;
	}

	public void setNodeHealth(NodeHealth nodeHealth) {
		this.nodeHealth = nodeHealth;
	}

	public NodeRegistry getNodeRegistry() {
		return nodeRegistry;
	}

	public void setNodeRegistry(NodeRegistry nodeRegistry) {
		this.nodeRegistry = nodeRegistry;
	}

	@Override
	public String toString() {
		return "NodeHandler: nodeRegistry " + nodeRegistry + " nodeHealth " + nodeHealth;
	}
}
