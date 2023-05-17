package io.tapdata.flow.engine.V2.node.hazelcast.controller;

import org.apache.commons.collections4.CollectionUtils;

import java.io.Serializable;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author samuel
 * @Description
 * @create 2023-05-11 10:40
 **/
public class NodeControlLayer implements Serializable {
	private static final long serialVersionUID = 4798982263744191565L;
	private final List<NodeController> nodeControllers;
	private AtomicBoolean finish;

	public NodeControlLayer(List<NodeController> nodeControllers) {
		this.nodeControllers = nodeControllers;
		this.finish = new AtomicBoolean(false);
	}

	public List<NodeController> getNodeControllers() {
		return nodeControllers;
	}

	public AtomicBoolean getFinish() {
		return finish;
	}

	public void run() {
		if (CollectionUtils.isEmpty(nodeControllers)) {
			return;
		}
		for (NodeController nodeController : nodeControllers) {
			nodeController.runningAndNotify();
		}
	}

	public boolean finish() {
		return this.finish.compareAndSet(false, true);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", NodeControlLayer.class.getSimpleName() + "[", "]")
				.add("nodeControllers=" + nodeControllers)
				.toString();
	}
}
