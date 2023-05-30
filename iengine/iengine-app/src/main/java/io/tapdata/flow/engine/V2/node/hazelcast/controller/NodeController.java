package io.tapdata.flow.engine.V2.node.hazelcast.controller;

import com.tapdata.tm.commons.dag.Node;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author samuel
 * @Description
 * @create 2023-05-10 17:34
 **/
public class NodeController implements Serializable {
	public final static int WAIT_RUN = 1;
	public final static int RUNNING = 2;
	public final static int FINISH = 3;
	private static final long serialVersionUID = -6420234322714249926L;

	private final AtomicInteger status = new AtomicInteger(WAIT_RUN);
	private Node<?> node;

	public NodeController(Node<?> node) {
		this.node = node;
	}

	public AtomicInteger getStatus() {
		return status;
	}

	public Node<?> getNode() {
		return node;
	}

	public void waitRun() {
		this.status.set(WAIT_RUN);
	}

	public void running() {
		this.status.set(RUNNING);
	}

	public void runningAndNotify() {
		this.status.set(RUNNING);
		synchronized (this.status) {
			this.status.notifyAll();
		}
	}

	public void finish() {
		this.status.set(FINISH);
	}
}
