package io.tapdata.aspect;

import com.tapdata.tm.commons.dag.Node;

public abstract class ThreadGroupAspect<T extends DataNodeAspect<?>> extends DataNodeAspect<T> {
    protected ThreadGroup threadGroup;
    protected String associateId;
    protected Node<?> node;

    public String getAssociateId() {
        return associateId;
    }

    public ThreadGroup getThreadGroup() {
        return threadGroup;
    }

    public Node<?> getNode() {
        return node;
    }

    public ThreadGroupAspect(Node<?> node, String associateId, ThreadGroup threadGroup) {
        super();
        this.threadGroup = threadGroup;
        this.associateId = associateId;
        this.node = node;
    }
}
