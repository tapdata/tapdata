package io.tapdata.flow.engine.V2.aspect;

import io.tapdata.entity.aspect.Aspect;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;

public abstract class NodeAspect<T extends NodeAspect<?>> extends Aspect {
	private HazelcastBaseNode node;
	public T node(HazelcastBaseNode node) {
		this.node = node;
		return (T) this;
	}

	public HazelcastBaseNode getNode() {
		return node;
	}

	public void setNode(HazelcastBaseNode node) {
		this.node = node;
	}
}
